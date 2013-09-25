(ns cravendb.indexengine
  (use [cravendb.core]
       [clojure.pprint]
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

(defn ex-error [prefix ex]
  (error prefix (.getMessage ex) (map #(.toString %1) (.getStackTrace ex))))

(defn refresh-indexes [engine]
  (try 
    (let [indexes-to-add 
          (filter #(not-any? 
                     (partial = (:id %1)) 
                        (map :id (:compiled-indexes engine))) 
                        (all-indexes (:db engine))) 
          new-indexes (concat (:compiled-indexes engine) 
                              (compile-indexes indexes-to-add (:db engine)))]

        (-> engine
        (assoc :compiled-indexes new-indexes)
        (assoc :indexes-by-name (into {} (for [i new-indexes] [(i :id) i])))))

    (catch Exception ex
      (ex-error "REFRESH FUCK" ex)
      engine)))


(defn remove-any-finished-chasers [engine]
  (info "Removing chasers that aren't needed")
  (assoc engine :chasers
        (filter #(not (realized? (:future %1))) (:chasers engine))))

(defn needs-a-new-chaser [engine index]
  (info "Checking if we need a new chaser for" (:id index))
   (and
    (not= 
     (indexing/last-indexed-etag (:db engine)) 
      (indexes/get-last-indexed-etag-for-index 
        (:db engine) 
        (:id index)))
      (not-any? 
        (partial = (:id index))
        (map :id (:chasers engine)))))

(defn create-chaser [index]
  (info "Starting a freaking chaser for " (:id index))
  {
   :id (:id index)
   :future (future (println "lol"))
   })

(defn start-new-chasers [engine]
  (info "Starting new chasers")
  (assoc engine :chasers
    (concat 
      (:chasers engine)
      (doall 
        (map 
          create-chaser 
          (filter 
            #(needs-a-new-chaser engine %1) 
            (:compiled-indexes engine)))))))

(defn indexes-which-are-up-to-date [engine]
  ;; Any indexes not in the chaser collection
  (:compiled-indexes engine))

(defn pump-indexes-at-head [engine]
  (try
    (indexing/index-documents! 
      (:db engine) 
      (indexes-which-are-up-to-date engine))
    (catch Exception ex
      (ex-error "INDEXING FUCK" ex)))
  engine) ;; no mutation here

(defn mark-pump-as-complete [engine]
  (debug "Index chaser complete")
  (assoc engine :running-pump false))

(defn pump-indexes [engine]
  (-> engine 
    remove-any-finished-chasers 
    start-new-chasers
    pump-indexes-at-head
    mark-pump-as-complete))

(defn try-pump-indexes [engine ea]
 (if (:running-pump engine) 
   engine
   (do
     (send ea pump-indexes)
     (assoc engine :running-pump true))))

(defn start-indexing [engine ea]
  (let [task (future 
        (loop []
          (try
            (send ea refresh-indexes)
            (send ea try-pump-indexes ea)
            (catch Exception e
              (ex-error "SHIT" e)))
          (Thread/sleep 50)
          (recur)))]
   (assoc engine :worker-future task)))

(defn stop-indexing [engine]
  (try
    (future-cancel (:worker-future engine))
    (assoc engine :worker-future nil)
    (catch Exception ex
      (ex-error "STOPPING FUCK" (pprint ex))
      engine
      )))

(defprotocol EngineOperations
  (start [this])
  (open-reader [this index-id])
  (stop [this])
  (close [this]))

(defrecord EngineHandle [ea]
  EngineOperations
  (start [this]
   (send ea start-indexing ea))
  (open-reader [this index-id]
    (.open-reader (get-in @ea [:indexes-by-name index-id :storage])))
  (stop [this]
    (debug "Stopping indexing agents")
   (send ea stop-indexing)
   (await ea))
  (close [this]
    (debug "Closing engine handle")
    (send ea close-engine)
    (await ea)))

(defn create-engine [db]
  (let [compiled-indexes (load-compiled-indexes db)]
    (EngineHandle.
      (agent {
              :chasers ()
              :db db
              :compiled-indexes compiled-indexes
              :indexes-by-name (into {} (for [i compiled-indexes] [(i :id) i])) 
              })))) 
