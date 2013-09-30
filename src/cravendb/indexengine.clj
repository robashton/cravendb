(ns cravendb.indexengine
  (:use [cravendb.core]
       [clojure.pprint]
       [clojure.tools.logging :only (info debug error)])
  (:import (java.io File File PushbackReader IOException FileNotFoundException ))
  (:require [cravendb.lucene :as lucene]
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
  (assoc index 
         :map (load-string (index :map))
         :filter (if (:filter index) (load-string (:filter index)) nil)))

(defn compile-indexes [indexes db]
  (map (comp 
         (partial open-storage-for-index (.path db))
         compile-index)
       indexes))

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

(defn refresh-indexes [{:keys [db compiled-indexes] :as engine}]
  (try 
    (let [indexes-to-add 
          (filter #(not-any? 
                     (partial = (:id %1)) 
                        (map :id compiled-indexes)) 
                        (all-indexes db))] 


      (if (not-empty indexes-to-add)
        (let [new-indexes (doall (concat compiled-indexes 
                            (compile-indexes indexes-to-add db)))]
          (debug "Loading new indexes from storage")
          (assoc engine :compiled-indexes new-indexes
            :indexes-by-name (into {} (for [i new-indexes] [(i :id) i]))))

        engine))
    (catch Exception ex
      (ex-error "REFRESH FUCK" ex)
      engine)))


(defn remove-any-finished-chasers [engine]
  (debug "Removing chasers that aren't needed")
  (assoc engine :chasers
        (filter #(not (realized? (:future %1))) (:chasers engine))))

(defn needs-a-new-chaser [engine index]
  (debug "Checking if we need a new chaser for" (:id index))
    (try
      (and
      (not= 
      (indexing/last-indexed-etag (:db engine)) 
        (indexes/get-last-indexed-etag-for-index 
          (:db engine) 
          (:id index)))
        (not-any? 
          (partial = (:id index))
          (map :id (:chasers engine))))
      (catch Exception e
        (ex-error "needs-a-new-chaser" e))))

(defn create-chaser [engine index]
  (info "Starting a freaking chaser for " (:id index))
  {
   :id (:id index)
   :future 
    (future
      (indexing/index-catchup! (:db engine) index))
   })

(defn indexes-which-require-a-chaser [engine]
  (filter 
    #(needs-a-new-chaser engine %1) 
    (:compiled-indexes engine)))

(defn start-new-chasers [engine]
  (debug "Starting new chasers")
  (assoc engine :chasers
    (doall
      (concat 
      (:chasers engine)
      (doall 
        (map #(create-chaser engine %1) 
          (indexes-which-require-a-chaser engine)))))))

(defn indexes-which-are-up-to-date [engine]
  (filter #(not-any? 
             (partial = (:id %1)) 
             (map :id (:chasers engine))) 
          (:compiled-indexes engine)))

(defn pump-indexes-at-head [engine]
  (try
    (indexing/index-documents! 
      (:db engine) 
      (indexes-which-are-up-to-date engine))
    engine
    (catch Exception ex
      (ex-error "INDEXING FUCK" ex))))

(defn mark-pump-as-complete [engine]
  (debug "Index chaser complete")
  (assoc engine :running-pump false))

(defn pump-indexes [engine]
  (try
    (-> engine 
    remove-any-finished-chasers 
    start-new-chasers
    pump-indexes-at-head
    mark-pump-as-complete)
   (catch Exception e
    (ex-error "pumping" e))))

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
              (ex-error "SHIT STARTING" e)))
          (Thread/sleep 50)
          (recur)))]
   (assoc engine :worker-future task)))

(defn stop-indexing [engine]
  (try
    (future-cancel (:worker-future engine))
    (assoc engine :worker-future nil)
    (catch Exception ex
      (ex-error "STOPPING FUCK" (pprint ex))
      engine)))

(defprotocol EngineOperations
  (start [this])
  (get-storage [this index-id])
  (stop [this])
  (close [this]))

(defrecord EngineHandle [ea]
  EngineOperations
  (start [this]
   (send ea start-indexing ea))
  (get-storage [this index-id]
    (get-in @ea [:indexes-by-name index-id :storage]))
  (stop [this]
    (debug "Stopping indexing agents")
   (send ea stop-indexing)
   (await ea))
  (close [this]
    (debug "Closing engine handle")
    (send ea close-engine)
    (await ea)))

(defn handle-agent-error [engine e]
  (ex-error "SHIT-IN-AGENT" e))

(defn create-engine [db]
  (let [compiled-indexes (load-compiled-indexes db)
        engine (agent {
              :chasers ()
              :db db
              :compiled-indexes compiled-indexes
              :indexes-by-name (into {} (for [i compiled-indexes] [(i :id) i])) 
              }) ]
    (set-error-handler! engine handle-agent-error)
    (EngineHandle. engine))) 
