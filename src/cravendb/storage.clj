(ns cravendb.storage
  (:use [clojure.tools.logging :only (info debug error)])
  (:require [clojure.core.incubator :refer [dissoc-in]]))

(:import 'org.iq80.leveldb.Options)
(:import 'org.iq80.leveldb.ReadOptions)
(:import 'org.iq80.leveldb.WriteOptions)
(:import 'org.iq80.leveldb.DBIterator)
(:import 'org.fusesource.leveldbjni.JniDBFactory)
(:import 'java.io.File)
(:import 'java.nio.ByteBuffer)

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

(defn safe-get [db k]
  (try
    (.get db k)
    (catch Exception e 
      (println e) 
      nil)))

(defprotocol Storage 
  (close [this])) 

(defrecord LevelTransaction [db options path]
  Storage
  (close [this]
    (debug "Closing the snapshot")
    (.close (.snapshot options))))

(defrecord LevelStorage [path db]
  Storage
  (close [this] 
    (debug "Closing the actual storage engine")
    (.close db) 
    nil))

(defn store [ops id data]
  (assoc-in ops [:cache id] (to-db data))) 

(defn delete [ops id]
  (assoc-in ops [:cache id] :deleted))

(defn get-blob [ops id]
  (let [cached (get-in ops [:cache id])]
    (if (= cached :deleted) nil
      (or cached 
          (if (:options ops)
            (.get (:db ops) (to-db id) (:options ops)) 
            (.get (:db ops) (to-db id)))))))

(defn get-integer [ops id]
  (from-db-int (get-blob ops id)))

(defn get-string [ops id]
  (from-db-str (get-blob ops id)))

(defn get-iterator [ops]
  (.iterator (:db ops) (:options ops)))

(defn commit! [ops]
  (with-open [batch (.createWriteBatch (:db ops))]
      (doseq [k (map #(vector  (first %) (second %)) (get ops :cache))]
        (let [id (k 0)
              value (k 1)]
          (if (= value :deleted)
            (.delete batch (to-db id))
            (.put batch (to-db id) value))))
      (let [wo (WriteOptions.)]
        (.sync wo true)
        (.write (:db ops) batch wo)))
  nil)

(defn ensure-transaction [ops]
  (debug "Opening transaction")
  (let [options (ReadOptions.)
        snapshot (.getSnapshot (:db ops))]
    (.snapshot options snapshot)
    (LevelTransaction. (:db ops) options (:path ops))))

(defn create-db [dir]
  (let [options (Options.)]
    (.createIfMissing options true)
    (.open (JniDBFactory/factory) (File. dir) options)))

(defn create-storage [dir]
  (LevelStorage. dir (create-db dir)))
