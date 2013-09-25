(ns cravendb.indexengine
  (use [cravendb.core]
       [clojure.tools.logging :only (info debug error)])
  (:import (java.io File File PushbackReader IOException FileNotFoundException ))
  (require [cravendb.lucene :as lucene]
           [cravendb.indexstore :as indexes]
           [cravendb.indexing :as indexing]))

(defn open-storage-for-index [path index]
  (let [storage (lucene/create-index (File. path (index :id)))]
    (-> index
      (assoc :storage storage)
      (assoc :writer (.open-writer storage)))))


(defn all-indexes [db]
  (with-open [tx (.ensure-transaction db)
              iter (.get-iterator tx)]
    (doall (map read-string (indexes/iterate-indexes iter)))))

(defn compile-index [index]
  (assoc index :map (load-string (index :map))))

(defn compile-indexes [indexes db]
  (doall (map (comp 
                (partial open-storage-for-index (.path db))
                compile-index)
              indexes)))

(defn load-compiled-indexes [db]
  (compile-indexes (all-indexes db) db))

(defn close-engine [engine]
  (debug "Closing the engine")
  (doseq [i (:compiled-indexes engine)] 
    (do
      (.close (i :writer)) 
      (.close (i :storage)))))

(defn get-compiled-indexes [handle] 
  (:compiled-indexes @(:ea handle)))

#_ (def compiled-indexes '({ :id "by_bar" }))
#_ (def all-indexes '({ :id "by_bar"}))

#_ (filter #(not-any? 
            (partial = (:id %1)) 
              (map :id compiled-indexes)) 
              all-indexes) 

(defn refresh-indexes [engine db]
  (try 
    (let [indexes-to-add 
          (filter #(not-any? 
                     (partial = (:id %1)) 
                        (map :id (:compiled-indexes engine))) 
                        (all-indexes db)) 
          new-indexes (concat (:compiled-indexes engine) 
                              (compile-indexes indexes-to-add db))]

        (-> engine
        (assoc :compiled-indexes new-indexes)
        (assoc :indexes-by-name (into {} (for [i new-indexes] [(i :id) i])))))

    (catch Exception ex
      (error "REFRESH FUCK" (.getMessage ex))
      engine)))


(defn run-index-chaser [engine db]
  ;; Tidy up any futures that have finished


  ;; Run chaser here and make a note of which indexes are being run
  (try
    (indexing/index-documents! db (:compiled-indexes engine))
    (catch Exception ex
      (error "INDEXING FUCK" ex)))

  ;; Run any indexes that need catching up in their own futures

  (debug "Index chaser complete")
  (assoc engine :running-chaser false))

(defn try-run-index-chaser [engine db ea]
 (if (:running-chaser engine) 
   engine
   (do
     (send ea run-index-chaser db)
     (assoc engine :running-chaser true))))

(defn start-indexing [engine db ea]
  (let [task (future 
        (loop []
          (try
            (debug "LOOP")
            (send ea refresh-indexes db)
            (send ea try-run-index-chaser db ea)
            (debug "END LOOP")
            (catch Exception e
              (error "SHIT" e)))
          (Thread/sleep 50)
          (recur)))]
   (assoc engine :worker-future task)))

(defn stop-indexing [engine db]
  (try
    (future-cancel (:worker-future engine))
    (assoc engine :worker-future nil)
    (catch Exception ex
      (error "STOPPING FUCK" (pprint ex))
      engine
      )))

(defprotocol EngineOperations
  (start [this db])
  (open-reader [this index-id])
  (stop [this db])
  (close [this]))

(defrecord EngineHandle [ea]
  EngineOperations
  (start [this db]
   (send ea start-indexing db ea))
  (open-reader [this index-id]
    (.open-reader (get-in @ea [:indexes-by-name index-id :storage])))
  (stop [this db]
    (debug "Stopping indexing agents")
   (send ea stop-indexing db)
   (await ea))
  (close [this]
    (debug "Closing engine handle")
    (send ea close-engine)
    (await ea)))

(defn create-engine [db]
  (let [compiled-indexes (load-compiled-indexes db)]
    (EngineHandle.
      (agent {
              :compiled-indexes compiled-indexes
              :indexes-by-name (into {} (for [i compiled-indexes] [(i :id) i])) 
              })))) 
