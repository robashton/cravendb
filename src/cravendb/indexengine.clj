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

(defprotocol Indexes
  (index [this id])
  (all [this])
  (refresh [this])
  (open-reader-for [this index])
  (setup [this compiled-indexes])
  (close [this]))

(defrecord IndexInstance []
  Indexes
  (all [this] (get this :compiled-indexes))
  (index [this id] (get this id))
  (refresh [this])
  (open-reader-for [this index]
    (.open-reader ((get this index) :storage)))
  (setup [this compiled-indexes]
    (-> this
      (assoc :compiled-indexes compiled-indexes)
      (into (for [i compiled-indexes] [(i :id) i]))))
  (close [this]
    (doseq [i (get this :compiled-indexes)] 
      (do
        (.close (i :writer)) 
        (.close (i :storage))))))

(defn load-from [db]
  (.setup 
    (IndexInstance. )
    (load-compiled-indexes db)))

(defn load-into [db]
  (assoc db :index-engine (atom (load-from db))))

(defn get-engine [db] @(get db :index-engine))
(defn get-compiled-indexes [db] (get (get-engine db) :compiled-indexes) )

(defn teardown [db]
  (.close (get-engine db)))

;; I need to use an agent for this as it's not thread safe
(defn refresh-indexes [db]
  (.close (get-engine db))
  (let [new-engine (load-from db)]
      (swap! (get db :index-engine) (fn [] new-engine))))

(defn setup [db]
  (let [loaded-db (load-into db)]
   (future 
    (loop []
      (Thread/sleep 100)
      (refresh-indexes loaded-db)
      (try        
        (indexing/index-documents! loaded-db (get-compiled-indexes loaded-db) )
        (catch Exception e
          (error e)))
      (recur)))
    loaded-db))


