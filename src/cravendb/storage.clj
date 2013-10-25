(ns cravendb.storage
  (:use [clojure.tools.logging :only (info debug error)])
  (:require [clojure.core.incubator :refer [dissoc-in]])
  (:import (org.iq80.leveldb Options ReadOptions WriteOptions DBIterator )
           (org.fusesource.leveldbjni JniDBFactory)
           (java.io File)
           (java.nio ByteBuffer)))

(defn to-db [input]
  (if (string? input)
   (.getBytes input "UTF-8")
   (if (integer? input)
    (.array (.putInt (ByteBuffer/allocate 4) input)))))

(defn from-db-str [input]
  (if (= input nil)
    nil
    (String. input "UTF-8")))

(defn from-db-int [input]
  (if (= input nil)
    0
    (.getInt (ByteBuffer/wrap input))))

(defn safe-get [db k options]
  (try
    (if options
      (.get db k options)   
      (.get db k)) 
    (catch Exception e 
      nil)))

(defrecord LevelTransaction [db options path]
  java.io.Closeable
  (close [this]
    (debug "Closing the snapshot")
    (.close (.snapshot options))))

(defrecord LevelStorage [path db]
  java.io.Closeable
  (close [this] 
    (debug "Closing the actual storage engine")
    (.close db) 
    nil))

(defrecord MemoryStorage [memory])
(defrecord MemoryTransaction [memory])

(defn store [ops id data]
  (assoc-in ops [:cache id] (to-db data))) 

(defn delete [ops id]
  (assoc-in ops [:cache id] :deleted))

(defmulti get-blob (fn [ops id] (if (:db ops) :disk :memory)))
(defmethod get-blob :disk [ops id]
  (let [cached (get-in ops [:cache id])]
    (if (= cached :deleted) nil
      (or cached 
        (safe-get (:db ops) (to-db id) (:options ops))))))

(defmethod get-blob :memory [ops id]
  (let [cached (get-in ops [:cache id])]
    (if (= cached :deleted) nil
      (or cached (get-in ops @[:memory id])))))

(defn get-integer [ops id]
  (from-db-int (get-blob ops id)))

(defn get-string [ops id]
  (from-db-str (get-blob ops id)))

(defn get-iterator [ops]
  (if (:options ops)
    (.iterator (:db ops) (:options ops))  
    (.iterator (:db ops))))

(defmulti commit! (fn [i] (if (:db i) :disk :memory)))
(defmethod commit! :disk [{:keys [db cache]}]
  (with-open [batch (.createWriteBatch db)]
    (doseq [[id value] cache]
      (if (= value :deleted)
        (.delete batch (to-db id))
        (.put batch (to-db id) value)))
    (let [wo (WriteOptions.)]
      (.sync wo true)
      (.write db batch wo)))
  nil)

(defmethod commit! :memory [{:keys[ memory cache]}]
  (swap! memory #(reduce (fn [m [k v]] (if (= :deleted v) (dissoc m k) (assoc m k v))) %1 cache)))

(defmulti ensure-transaction (fn [ops] (if (:db ops) :disk :memory)))
(defmethod ensure-transaction :disk [ops]
  (debug "Opening transaction") 
  (let [options (ReadOptions.)
          snapshot (.getSnapshot (:db ops))]
      (.snapshot options snapshot)
      (LevelTransaction. (:db ops) options (:path ops))))

(defmethod ensure-transaction :memory [ops]
  (debug "Opening transaction") 
  (MemoryTransaction. (:memory ops)))

(defn create-db [dir]
  (let [options (Options.)]
    (.createIfMissing options true)
    (.open (JniDBFactory/factory) (File. dir) options)))

(defn create-storage [dir]
  (LevelStorage. dir (create-db dir)))

(defn create-memory-storage []
  (MemoryStorage. (atom {})))
