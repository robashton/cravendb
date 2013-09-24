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

(defn get-engine [db] 
  @(get db :index-engine))

(defn close-engine [engine]
  (doseq [i (:compiled-indexes engine)] 
    (do
      (.close (i :writer)) 
      (.close (i :storage)))))

(defprotocol Resource
  (close [this]))

(defrecord IndexEngine [db]
  Resource
  (close [this] (close-engine this)))

(defn load-from [db]
  (let [compiled-indexes (load-compiled-indexes db)]
    (-> 
      (IndexEngine. db)
        (assoc :compiled-indexes compiled-indexes )
        (assoc :indexes-by-name 
          (into {} (for [i compiled-indexes] [(i :id) i]))))))

(defn load-into [db]
  (assoc db :index-engine (agent (load-from db))))

(defn reader-for-index [db index]
 (.open-reader (get-in (get-engine db) [:indexes-by-name index :storage])))

(defn get-compiled-indexes [db] 
  (get (get-engine db) :compiled-indexes) )

(defn teardown [db]
  (future-cancel (get db :index-engine-worker))
  (close-engine (get-engine db)))

(defn refresh-indexes [engine db]
  (info "ABOUT TO DO THIS SHIT")
  (try 
    (info "CLOSING")
    (close-engine engine)
    (info "OPENING")
    (load-from db)
    (catch Exception e
      (info e))))

(defn run-index-chaser [engine db]
  (indexing/index-documents! db (:compiled-indexes engine))
  engine)

(defn start [db]
  (let [loaded-db (load-into db)] 
    (let [task (future 
        (loop []
          (try
            (send (:index-engine loaded-db) refresh-indexes loaded-db)
            (send (:index-engine loaded-db) run-index-chaser loaded-db)
            (catch Exception e
              (error e)))
          (Thread/sleep 100)
          (recur))) ]
      (assoc loaded-db :index-engine-worker task))))
