(ns cravendb.storage
  (:use [clojure.tools.logging :only (info debug error)])
  (:require [clojure.core.incubator :refer [dissoc-in]]
            [cravendb.core :refer [zero-synctag integer-to-synctag synctag-to-integer]])
  (:import (org.iq80.leveldb Options ReadOptions WriteOptions DBIterator)
           (org.fusesource.leveldbjni JniDBFactory)
           (java.io File)
           (java.nio ByteBuffer)))

(def last-synctag-key "__last-synctag")

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

(defrecord LevelTransaction [db options path last-synctag]
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

(defrecord MemoryStorage [memory]
  java.io.Closeable
  (close [this]))
(defrecord MemoryTransaction [path snapshot memory last-synctag]
  java.io.Closeable
  (close [this]))
(defrecord MemoryIterator [snapshot memory start]
  java.io.Closeable
  (close [this]))

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
      (or cached (get (or (:snapshot ops) @(:memory ops)) id)))))

(defn get-integer [ops id]
  (from-db-int (get-blob ops id)))

(defn get-string [ops id]
  (from-db-str (get-blob ops id)))

(defrecord StorageIterator [inner]
  java.io.Closeable
  (close [this] (.close inner)))

(defn expand-iterator-str [i]
  { :k (from-db-str (.getKey i))
    :v (from-db-str (.getValue i)) })

(defmulti as-seq (fn [i] (if (:inner i) :disk :memory)))
(defmethod as-seq :disk [iter]
  (->> (iterator-seq (:inner iter))
   (map expand-iterator-str))) 
(defmethod as-seq :memory [iter]
  (map (fn [i] {:k (key i) :v (from-db-str (val i))}) 
       (drop-while #(> 0 (compare (key %1) @(:start iter))) (or (:snapshot iter) @(:memory iter)))))


(defmulti seek (fn [i v] (if (:inner i) :disk :memory)))
(defmethod seek :disk [iter value]
  (.seek (:inner iter) (to-db value))
  iter)
(defmethod seek :memory [iter value]
  (swap! (:start iter) (fn [i] value)))

(defmulti get-iterator (fn [i] (if (:db i) :disk :memory)))

(defmethod get-iterator :disk [ops]
  (StorageIterator.
     (if (:options ops)
        (.iterator (:db ops) (:options ops))  
        (.iterator (:db ops)))))

(defmethod get-iterator :memory [ops]
  (MemoryIterator. (:snapshot ops) (:memory ops) (atom nil)))

(defn last-synctag-in
  [storage]
  (or (get-string storage last-synctag-key) (zero-synctag)) )

(defn bootstrap-storage
  [storage]
  (assoc storage
    :closed (agent false)
    :last-synctag (atom (synctag-to-integer (last-synctag-in storage)))))

(defn next-synctag [{:keys [last-synctag]}]
  (integer-to-synctag (swap! last-synctag inc)))

(defmulti commit! (fn [i] (if (:db i) :disk :memory)))
(defmethod commit! :disk [{:keys [db cache] :as tx}]
  (with-open [batch (.createWriteBatch db)]
    (doseq [[id value] cache]
      (if (= value :deleted)
        (.delete batch (to-db id))
        (.put batch (to-db id) value)))
    (.put batch 
      (to-db last-synctag-key) 
      (to-db (integer-to-synctag @(:last-synctag tx))))
    (let [wo (WriteOptions.)]
      (.sync wo true)
      (.write db batch wo))))

(defmethod commit! :memory [{:keys[ memory cache] :as tx}]
  (swap! memory 
         #(reduce (fn [m [k v]] 
                    (if (= :deleted v) 
                      (dissoc m k) 
                      (assoc m k v))) 
                  %1 (assoc cache
                            last-synctag-key
                            (to-db (integer-to-synctag @(:last-synctag tx)))))))

(defmulti ensure-transaction (fn [ops] (if (:db ops) :disk :memory)))
(defmethod ensure-transaction :disk [ops]
  (debug "Opening transaction") 
  (let [options (ReadOptions.)
          snapshot (.getSnapshot (:db ops))]
      (.snapshot options snapshot)
      (LevelTransaction. (:db ops) options (:path ops) (:last-synctag ops))))

(defmethod ensure-transaction :memory [ops]
  (debug "Opening transaction") 
  (MemoryTransaction. (:path ops) @(:memory ops) (:memory ops) (:last-synctag ops)))

(defn create-db [dir]
  (let [options (Options.)]
    (.createIfMissing options true)
    (.open (JniDBFactory/factory) (File. dir) options)))

(defn create-storage [dir]
  (bootstrap-storage (LevelStorage. dir (create-db dir))))

(defn create-in-memory-storage []
  (bootstrap-storage (MemoryStorage. (atom (sorted-map)))))
