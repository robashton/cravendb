(ns cravendb.indexengine
  (:use [cravendb.core]
       [clojure.pprint]
       [clojure.core.async] 
       [clojure.tools.logging :only (info debug error)])
  (:import (java.io File File PushbackReader IOException FileNotFoundException ))
  (:require [cravendb.lucene :as lucene]
           [cravendb.storage :as s]
           [clojure.core.incubator :refer [dissoc-in]]
           [me.raynes.fs :as fs]
           [cravendb.indexstore :as indexes]
           [cravendb.defaultindexes :as di]
           [cravendb.indexing :as indexing]
           [cravendb.tasks :as tasks]
           [clojure.edn :as edn]))

(defn index-uid [index]
  (str (:id index) "-" (or (:synctag index) "")))

(defn open-storage-for-index [path index]
  (let [storage (if path (lucene/create-index (File. path (index-uid index)))
                         (lucene/create-memory-index))]
    (-> index
      (assoc :storage storage)
      (assoc :writer (lucene/open-writer storage)))))

(defn close-storage-for-index [{:keys [writer storage]}]
  (.close writer)
  (.close storage))

(defn close-open-indexes [{:keys [indexes chaser-indexes deleted-indexes]}]
  (doseq [i deleted-indexes]
    (do (info "closing" (:id i)) (close-storage-for-index i)))
  (doseq [[k i] indexes] 
    (do (info "closing" k) (close-storage-for-index i)))
  (doseq [[k i] chaser-indexes] 
    (do (info "closing" k) (close-storage-for-index i)))) 

(defn read-index-data [db index]
  (assoc index :synctag (indexes/synctag-for-index db (:id index))))

(defn all-indexes [db]
  (with-open 
    [tx (s/ensure-transaction db)
     iter (s/get-iterator tx)]
    (doall (map (partial read-index-data tx) (indexes/iterate-indexes iter)))))

(defn compile-index [index]
  (assoc index 
         :map (load-string (index :map))
         :filter (if (:filter index) (load-string (:filter index)) nil)))

(defn into-id-map [indexes]
  (into {} (for [i indexes] [(:id i) i])))
  
(defn initial-indexes [db]
  (into-id-map
    (map (partial open-storage-for-index (:path db))  
       (concat (di/all) (map compile-index (all-indexes db))))))

(defn prepare-index [db index]
  (open-storage-for-index (:path db) (read-index-data db (compile-index index))))

(defn initial-state [{:keys [db] :as engine}]
  (assoc engine
    :indexes (initial-indexes db)))

(defn go-index-some-stuff [{:keys [db indexes command-channel]}]
  (go 
    (info "indexing stuff for indexes" (map (comp :id val) indexes))
    (loop [num-indexed 1] 
      (if (> num-indexed 0) 
        (recur (indexing/index-documents! db (map val indexes)))))
    (info "done indexing stuff")
    (>! command-channel { :cmd :notify-finished-indexing})))

(defn main-indexing-process [state]
  (if (:indexing-channel state)
    (assoc state :work-peding true)
    (assoc state :indexing-channel (go-index-some-stuff state))))

(defn main-indexing-process-ended [{:keys [command-channel] :as state}]
  (if-let [deleted (:deleted-indexes state)]
    (doseq [i deleted] (close-storage-for-index i)))
  (if (:work-pending state) 
    (do
      (info "received indexing request, queuing")
      (put! command-channel { :cmd :schedule-indexing })))
  (-> state
    (dissoc :work-pending)
    (dissoc :indexing-channel)
    (dissoc :deleted-indexes)))

(defn remove-existing-index [state id]
  (if-let [index (or (get-in state [:indexes id]) (get-in state [:chaser-indexes id]))]
    (-> state
      (dissoc-in [:indexes id])
      (dissoc-in [:chaser-indexes id])
      (update-in [:deleted-indexes] conj index))
    state))

(defn add-new-index [{:keys [db] :as state} {:keys [id] :as index}]
  (info "adding new index to engine" id)
  (let [prepared-index (prepare-index db index)]
   (-> state
    (remove-existing-index id)
    (assoc-in [:indexes id] prepared-index)
    (main-indexing-process))))

(defn storage-request [state {:keys [id cb]}]
  (go (>! cb (or (get-in state [:indexes id :storage])
                 (get-in state [:chaser-indexes id :storage]))))
  state)

(defn go-catch-up [{:keys [db command-channel]} index]
  (go 
    (loop [amount 1] 
      (if (> amount 0)
       (recur (indexing/index-catchup! db index)) ))
      (>! command-channel { :cmd :chaser-finished :data index})))

(defn add-chaser [{:keys [db] :as state} index]
  (let [prepared-index (prepare-index db index)]
    (-> state
      (assoc-in [:chaser-indexes (:id prepared-index)] prepared-index)
      (assoc-in [:chasers (:id prepared-index)] (go-catch-up state prepared-index)))))

(defn finish-chaser 
  [{:keys [command-channel] :as state} {:keys [id] :as index}]
  (put! command-channel {:cmd :schedule-indexing}) 
  (-> state
    (dissoc-in [:chasers id])
    (dissoc-in [:chaser-indexes id])
    (assoc-in [:indexes id] index)))

(defn wait-for-chasers [state]
   (if-let [chasers (:chasers state)]
     (doseq [[i c] chasers] (<!! c))))

(defn wait-for-main-indexing [state]
  (if-let [main-indexing (:indexing-channel state)]
    (<!! (:indexing-channel state))))

(defn go-index-head [_ {:keys [command-channel] :as engine}]
  (go 
    (loop [state (initial-state engine)]
    (if-let [{:keys [cmd data]} (<! command-channel)]
     (do
       (recur (case cmd
         :schedule-indexing (main-indexing-process state)
         :notify-finished-indexing (main-indexing-process-ended state)
         :removed-index state ;; WUH OH
         :new-index (add-chaser state data)
         :chaser-finished (finish-chaser state data)
         :storage-request (storage-request state data))))
      (do
        (info "waiting for main index process")
        (wait-for-main-indexing state)
        (wait-for-chasers state)
        (close-open-indexes state))))))

(defn start [{:keys [event-loop] :as engine}]
  (swap! event-loop #(go-index-head %1 engine))) 

(defn stop 
  [{:keys [command-channel event-loop]}]
  (close! command-channel)
  (<!! @event-loop)
  (info "finished doing everything"))

(defrecord EngineHandle [db command-channel event-loop]
  java.io.Closeable
  (close [this]
    (stop this)))

(defn create [db] 
  (EngineHandle. db (chan) (atom nil)))

(defn notify-of-work [engine]
 (go (>! (:command-channel engine) {:cmd :schedule-indexing})))

(defn notify-of-new-index [engine index]
  (go (>! (:command-channel engine) {:cmd :new-index :data index})))

(defn notify-of-removed-index [engine index]
  (go (>! (:command-channel engine) {:cmd :removed-index :data index})))

;; Don't bleed async details to rest of app (yet)
;; There is hopefully a better way
(defn get-index-storage [{:keys [command-channel]} id]
  (let [result-channel (chan)] 
    (go (>! command-channel {:cmd :storage-request :data {:id id :cb result-channel}}))
    (<!! result-channel))) 
