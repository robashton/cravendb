(ns cravendb.levelstorage
  (:use [cravendb.core])
  (:require 
    [clojure.edn :as edn] 
    [cravendb.storageops :refer [Reader Writer Iterator Storage]]
    [clojure.tools.logging :refer [info debug error]])
  (:import (org.iq80.leveldb Options ReadOptions WriteOptions DBIterator)
           (org.fusesource.leveldbjni JniDBFactory)
           (java.io File)
           (java.nio ByteBuffer)))

(defn safe-get [db k options]
  (try
    (if options
      (.get db k options)   
      (.get db k)) 
    (catch Exception e 
      nil)))

(defn to-db [v]
  (with-open [stream (java.io.ByteArrayOutputStream.)] 
    (binding [*out* (clojure.java.io/writer stream)]
      (pr v)
      (.flush *out*))
    (.toByteArray stream)))

(defn from-db [v]
  (if (nil? v) nil
   (with-open [reader (java.io.PushbackReader.
                          (clojure.java.io/reader 
                            (java.io.ByteArrayInputStream. v)))]
    (edn/read reader))))

(defn expand-iterator-str [i]
  { :k (from-db (.getKey i))
    :v (from-db (.getValue i)) })

(defn commit! [{:keys [db cache] :as tx}]
  (with-open [batch (.createWriteBatch db)]
      (doseq [[id value] cache]
        (if (= value :deleted)
          (.delete batch (to-db id))
          (.put batch (to-db id) (to-db value))))
      (let [wo (WriteOptions.)]
        (.sync wo true)
        (.write db batch wo)))) 

(defn from-storage [ops id]
  (from-db (safe-get (:db ops) (to-db id) (:options ops)))) 

(defrecord LevelIterator [inner]
  java.io.Closeable
  Iterator
  (seek! [this value] (.seek inner (to-db value))) 
  (as-seq [this]
    (->> (iterator-seq inner) (map expand-iterator-str)))   
  (close [this] (.close inner)))

(defn get-iterator [ops]
  (LevelIterator.
     (if (:options ops)
        (.iterator (:db ops) (:options ops))  
        (.iterator (:db ops))))) 

(defrecord LevelTransaction [db options path]
  java.io.Closeable
  Writer
  Reader
  (open-iterator [this] (get-iterator this))
  (from-db [this id] (from-storage this id))
  (commit! [this] (commit! this))
  (close [this]
    (debug "Closing the snapshot")
    (.close (.snapshot options))))

(defrecord LevelStorage [path db]
  java.io.Closeable
  Reader
  Storage
  (ensure-transaction [ops]
    (debug "Opening transaction") 
    (let [options (ReadOptions.)
          snapshot (.getSnapshot (:db ops))]
      (.snapshot options snapshot)
      (LevelTransaction. (:db ops) options (:path ops)))) 
  (from-db [this id] (from-storage this id))
  (open-iterator [this] (get-iterator this))
  (close [this] 
    (debug "Closing the actual storage engine")
    (.close db) 
    nil)) 

(defn create-db [dir]
  (let [options (Options.)]
    (.createIfMissing options true)
    (.open (JniDBFactory/factory) (File. dir) options)))

(defn create [path]
  (LevelStorage. path (create-db path)))
