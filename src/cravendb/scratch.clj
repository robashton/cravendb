(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.tools.logging :only (info debug error)]
        [clojure.data.codec.base64]
        [clojure.core.async])
  (:import (java.io File File PushbackReader IOException FileNotFoundException ))
  (:require [cravendb.lucene :as lucene]
           [cravendb.storage :as s]
           [cravendb.documents :as docs]
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
  (with-open [tx (s/ensure-transaction db)
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

(defn create-engine [db] 
  { :db db
    :command-channel (chan)
    :event-loop (atom nil) })

(defn schedule-removal [state index]
  (update-in state [:pending-removal] conj index))

(defn go-index-some-stuff [{:keys [db indexes command-channel]}]
  (go 
    (info "indexing stuff for indexes" (map (comp val) indexes))
    (indexing/index-documents! db (map val indexes))
    (info "done indexing stuff")
    (>! command-channel { :cmd :notify-finished-indexing})))

(defn go-catch-up [index state]
  (go
    (info "running an index catch-up operation")
    (Thread/sleep 5000)
    (info "index is caught up")))

(defn main-indexing-process [state]
  (if (:indexing-channel state)
    (do (info "ignoring this request yo") state)
    (assoc state :indexing-channel (go-index-some-stuff state))))

(defn remove-dead-indexes [state]
    ;; Close the writers for these indexes
    ;; Dissoc them from the state
    ;; Great Success
  state
  )

(defn add-caught-up-indexes [state]
  ;; Not sure how I'm going to synchronise this)
  ;; Maybe a "close enough" approach
  ;; Start indexing from the smallest synctag we have
  state)

(defn create-chaser [state index]
  (update-in state [:chasers] conj (go-catch-up state index)))

(defn main-indexing-process-ended [state]
  (-> state
    (dissoc :indexing-channel)
    (remove-dead-indexes)
    (add-caught-up-indexes)))

(defn be-prepared [_ {:keys [command-channel] :as engine}]
  (go (loop [state (initial-state engine)]
    (if-let [{:keys [cmd data]} (<! command-channel)]
     (do
       (recur (case cmd
         :schedule-indexing (main-indexing-process state)
         :notify-finished-indexing (main-indexing-process-ended state)
         :new-index (create-chaser engine state)
         :removed-index (schedule-removal state data))))
      (do
        (info "waiting for main index process")
        (if-let [main-indexing (:indexing-channel state)]
          (<!! (:indexing-channel state)))
        ;; Definitely need a way to cancel these, even if it's a global atom
        (info "waiting for chasers")
        (if-let [chasers (:chasers state)]
          (doseq [c chasers] (<!! c)))
        (info "I would be closing resources here"))))))

(defn start [{:keys [event-loop] :as engine}]
  (swap! event-loop #(be-prepared %1 engine))) 

(defn stop 
  [{:keys [command-channel event-loop]}]
  (close! command-channel)
  (<!! @event-loop)
  (info "finished doing everything"))

(def current (atom nil))

(def test-index
  { :id "test-index" :map "(fn [doc] { \"title\" (doc :foo) })"})

(defn test-start [e]
  (let [db (s/create-in-memory-storage)
        engine (create-engine db)]
    (with-open [tx (s/ensure-transaction db)]
      (-> tx
        (indexes/put-index test-index (s/next-synctag tx))
        (s/commit!)))
    (start engine)
    { :db db
      :engine engine}))

(defn test-stop [{:keys [db engine]}]
  (stop engine)
  (.close db)
  nil)

(defn test-restart [e]
  (if e (test-stop e))
  (test-start e))


#_ (swap! current test-start)
#_ (swap! current test-stop)
#_ (swap! current test-restart)

#_ (with-open [tx (s/ensure-transaction (:db @current))]
     (-> tx
       (docs/store-document "doc-1" { :foo "bar" } { :synctag (s/next-synctag tx)})
       (s/commit!)))


xjkc
#_ (indexes/get-last-indexed-synctag-for-index (:db @current) (:id test-index))

