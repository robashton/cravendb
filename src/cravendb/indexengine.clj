(ns cravendb.indexengine
  (:use [cravendb.core]
       [clojure.pprint]
       [clojure.tools.logging :only (info debug error)])
  (:import (java.io File File PushbackReader IOException FileNotFoundException ))
  (:require [cravendb.lucene :as lucene]
           [cravendb.storage :as s]
           [me.raynes.fs :as fs]
           [cravendb.indexstore :as indexes]
           [cravendb.defaultindexes :as di]
           [cravendb.indexing :as indexing]
           [cravendb.tasks :as tasks]
           [clojure.edn :as edn]))

(defn storage-path-for-index [index]
  (str (:id index) "-" (or (:synctag index) "")))

(def index-queue "indexengine")
(def index-queue-handlers
  {
   :delete-index-data 
   (fn [{:keys [path] :as db} index]
     (if path
       (fs/delete-dir (File. (path (storage-path-for-index index)))))
    )})

(defn open-storage-for-index [path index]
  (let [storage (if path (lucene/create-index (File. path (storage-path-for-index index)))
                         (lucene/create-memory-index))]
    (-> index
      (assoc :storage storage)
      (assoc :writer (lucene/open-writer storage)))))

(defn read-index-data [tx index]
  (assoc index :synctag (indexes/synctag-for-index tx (:id index))))

(defn all-indexes [db]
  (try
    (with-open [tx (s/ensure-transaction db)
              iter (s/get-iterator tx)]
    (doall (map (partial read-index-data tx) (indexes/iterate-indexes iter))))
    (catch Exception ex
      (debug "WTF" ex))))

(defn ex-error [prefix ex]
  (error prefix (ex-expand ex)))

(defn compile-index [index]
  (assoc index 
         :map (load-string (index :map))
         :filter (if (:filter index) (load-string (:filter index)) nil)))

(defn map-indexes-by-id [indexes]
 (into {} (for [i indexes] [(:id i) i])))

(defn load-initial-indexes [db]
  (map-indexes-by-id 
    (map (partial open-storage-for-index (:path db))  
       (concat (di/all) (map compile-index (all-indexes db))))))

(defn close-engine [engine]
  (debug "Closing the engine")
  (doseq [[id i] (:compiled-indexes engine)] 
    (do
      (.close (i :writer)) 
      (.close (i :storage)))))

(defn compiled-indexes [handle] 
  (map val (:compiled-indexes @(:ea handle))))

(defn prepare-index [id db]
  (open-storage-for-index 
    (:path db) 
    (compile-index (indexes/load-index db id)) ))

(defn remove-index-from-engine [engine id ea]
  (if-let [i (get-in engine [:compiled-indexes id])]
    (do
      (.close (i :writer)) 
      (.close (i :storage))
      (dissoc-in engine [:compiled-indexes id]))
    engine))

(defn add-index-to-engine [engine id ea]
  (-> engine
    (remove-index-from-engine id ea)
    (assoc-in [:compiled-indexes id (prepare-index id (:db engine))

(defn create-chaser [engine index]
  (debug "Starting a freaking chaser for " (:id index))
  {
   :id (:id index)
   :future 
    (future
      (indexing/index-catchup! (:db engine) index)) })

(defn pump-indexes-at-head! [engine]
  (indexing/index-documents! 
    (:db engine) 
    (indexes-which-are-up-to-date engine))
  engine)

(defn mark-pump-as-complete [engine]
  (debug "Index chaser complete")
  (assoc engine :running-pump false))

(defn pump-indexes! [engine]
  (-> engine 
    refresh-indexes!
    pump-indexes-at-head!
    mark-pump-as-complete))

(defn try-pump-indexes [engine ea]
 (if (:running-pump engine) 
   engine
   (do
     (send ea pump-indexes!)
     (assoc engine :running-pump true))))

(defn start-background-tasks [{:keys [db] :as engine}]
  (let [task (future 
    (loop []
      (tasks/pump db index-queue index-queue-handlers)
      (Thread/sleep 1000) ; teehee
      (recur)))]
   (assoc engine :background-future task)))

(defn wait-for [f]
  (loop []
    (while (not (future-done? f))
      (Thread/sleep 10))))

(defn stop-background-tasks [engine]
   (future-cancel (:background-future engine))
   (wait-for (:background-future engine))
   (dissoc engine :background-future))

(defn start-indexing [engine ea]
  (let [task (future 
    (loop []
      (send ea try-pump-indexes ea)
      (Thread/sleep 50)
      (recur)))]
   (assoc engine :worker-future task)))

(defn stop-indexing [engine]
  (future-cancel (:worker-future engine))
  (wait-for (:worker-future engine))
  (doseq [i (:chasers engine)]
    (wait-for (:future i)))
  (assoc engine :worker-future nil))

(defrecord EngineHandle [ea]
  java.io.Closeable
  (close [this]
    (debug "Closing engine handle")
    (send ea close-engine)
    (await ea)))

(defn start [ops]
  (send (:ea ops) start-indexing (:ea ops))
  (send (:ea ops) start-background-tasks))

(defn add-index [id]
  (send (:ea ops) add-index-to-engine (:ea ops)))

(defn remove-index [id]
  (send (:ea ops) remove-index-from-engine (:ea ops)))

(defn get-index-storage [ops index-id]
  (get-in @(:ea ops) [:compiled-indexes index-id :storage]))

(defn stop [ops]
 (debug "Stopping indexing agents")
 (send (:ea ops) stop-indexing)
 (send (:ea ops) stop-background-tasks)
 (await (:ea ops)))

(defn handle-agent-error [engine e]
  (ex-error "Indexing engine fell over" e))

(defn create-engine [db]
  (let [engine 
        (agent {
          :chasers ()
          :db db
          :compiled-indexes (load-initial-indexes db) })]
    (set-error-handler! engine handle-agent-error)
    (EngineHandle. engine))) 
