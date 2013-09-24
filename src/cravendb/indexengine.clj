(ns cravendb.indexengine
  (use [cravendb.core]
       [clojure.tools.logging :only (info error)])
  (:import (java.io File File PushbackReader IOException FileNotFoundException ))
  (require [cravendb.lucene :as lucene]
           [cravendb.indexstore :as indexes]
           [cravendb.indexing :as indexing]))

(defn open-storage-for-index [path index]
  (let [storage (lucene/create-index (File. path (index :id)))]
    (-> index
      (assoc :storage storage)
      (assoc :writer (.open-writer storage)))))

(defn compile-index [index]
  (assoc index :map (load-string (index :map))))

(defn load-compiled-indexes [db]
  (with-open [iter (.get-iterator db)]
    (doall (map 
             (comp 
                (partial open-storage-for-index (.path db))
                compile-index 
                read-string) 
              (indexes/iterate-indexes iter)))))

(defn close-engine [engine]
  (doseq [i (:compiled-indexes engine)] 
    (do
      (.close (i :writer)) 
      (.close (i :storage)))))

(defprotocol Resource
  (close [this]))

(defrecord EngineHandle [ea]
  Resource
  (close [this]
    (send ea close-engine)))

(defn create-engine [db]
  (let [compiled-indexes (load-compiled-indexes db)]
    (EngineHandle.
      (agent {
              :compiled-indexes compiled-indexes
              :indexes-by-name (into {} (for [i compiled-indexes] [(i :id) i])) 
              })))) 

;; This is not currently safe
(defn reader-for-index [handle index]
 (.open-reader (get-in @(:ea handle) [:indexes-by-name index :storage])))

(defn get-compiled-indexes [handle] 
  (:compiled-indexes @(:ea handle)))

(defn refresh-indexes [engine db]
  (try 
    (close-engine engine)
    (let [compiled-indexes (load-compiled-indexes db)]
      (-> engine
        (assoc :compiled-indexes compiled-indexes)
        (assoc :indexes-by-name (into {} (for [i compiled-indexes] [(i :id) i])))))
    (catch Exception e
      (info "REFRESH FUCK" e))))

(defn run-index-chaser [engine db]
  ;; Tidy up any futures that have finished

  ;; Run chaser here and make a note of which indexes are being run
  (try
    (indexing/index-documents! db (:compiled-indexes engine))
    (catch Exception ex
      (info "INDEXING FUCK" ex)))

  ;; Run any indexes that need catching up in their own futures

  engine)

(defn start-indexing [engine db ea]
  (let [task (future 
        (loop []
          (try
            (send ea refresh-indexes db)
            (send ea run-index-chaser db)
            (catch Exception e
              (error e)))
          (Thread/sleep 100)
          (recur)))]
   (assoc engine :worker-future task)))

(defn stop-indexing [engine db]
  (try
    (future-cancel (:worker-future engine))
    (assoc engine :worker-future nil)
    (catch Exception ex
      (info "STOPPING FUCK" ex)
      engine
      )))

(defn stop [db handle]
  (send (:ea handle) stop-indexing db))

(defn start [db handle]
  (send (:ea handle) start-indexing db (:ea handle)))
