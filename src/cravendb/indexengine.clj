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

(defn start-background-indexing [db]
  (future 
    (loop []
      (Thread/sleep 100)
      (try
        (indexing/index-documents! db (db [:compiled-indexes]))
        (catch Exception e
          (error e)))
      (recur))))

(defprotocol StoragePlugin
  (close [this]))

(defrecord IndexInstance [compiled-indexes]
  StoragePlugin
  (close [this]
    (doseq [i (get this :compiled-indexes)] 
      (do
        (.close (i :storage))
        (.close (i :writer))))))

(defn load-from [db]
  (IndexInstance. (load-compiled-indexes db)))
