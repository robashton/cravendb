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

(defn read-index-data [tx index]
  (assoc index :synctag (indexes/synctag-for-index tx (:id index))))

(defn all-indexes [db]
  (with-open 
    [tx (s/ensure-transaction db)
     iter (s/get-iterator tx)]
    (doall (map (partial read-index-data tx) (indexes/iterate-indexes iter)))))

(defn compile-index [index]
  (assoc index 
         :map (load-string (index :map))
         :filter (if (:filter index) (load-string (:filter index)) nil)))

(defn into-uid-map [indexes]
  (into {} (for [i indexes] [(index-uid i) i])))
  
(defn initial-indexes [db]
  (into-uid-map
    (map (partial open-storage-for-index (:path db))  
       (concat (di/all) (map compile-index (all-indexes db))))))

(defn initial-state [{:keys [db] :as engine}]
  (assoc engine
    :indexes (initial-indexes db)))


(defn schedule-removal [state index]
  (update-in state [:pending-removal] conj index))

(defn go-index-some-stuff [{:keys [db indexes command-channel]}]
  (go 
    (info "indexing stuff for indexes" (map (comp :id val) indexes))
    (while (not= (s/last-synctag-in db) 
                 (indexing/last-indexed-synctag db)) 
      (indexing/index-documents! db (map val indexes)))
    (info "done indexing stuff")
    (>! command-channel { :cmd :notify-finished-indexing})))

(defn main-indexing-process [state]
  (if (:indexing-channel state)
    (do (info "ignoring this request yo") state)
    (assoc state :indexing-channel (go-index-some-stuff state))))

(defn main-indexing-process-ended [state]
  (-> state
    (dissoc :indexing-channel)))

(defn add-new-index [{:keys [db] :as state} index]
  (info "adding new index to engine" (index-uid index))
  (assoc-in state [:indexes (index-uid index)]
            (open-storage-for-index (:path db) (compile-index index))))

(defn go-index-head [_ {:keys [command-channel] :as engine}]
  (go (loop [state (initial-state engine)]
    (if-let [{:keys [cmd data]} (<! command-channel)]
     (do
       (recur (case cmd
         :schedule-indexing (main-indexing-process state)
         :notify-finished-indexing (main-indexing-process-ended state)
         :removed-index state ;; WUH OH
         :new-index (add-new-index state data))))
      (do
        (info "waiting for main index process")
        (if-let [main-indexing (:indexing-channel state)]
          (<!! (:indexing-channel state)))
        (info "I would be closing resources here"))))))

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
