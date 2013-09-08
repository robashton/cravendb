(ns cravendb.storage
  (require [clojure.core.incubator :refer [dissoc-in]]))

(import 'org.iq80.leveldb.Options)
(import 'org.iq80.leveldb.ReadOptions)
(import 'org.iq80.leveldb.DBIterator)
(import 'org.fusesource.leveldbjni.JniDBFactory)
(import 'java.io.File)
(import 'java.nio.ByteBuffer)

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

(defprotocol Transaction
  (store [this id data])
  (delete [this id])
  (get-blob [this id])
  (get-integer [this id])
  (get-string [this id])
  (get-iterator [this])
  (commit [this]))

(defprotocol Storage 
  (close [this]) 
  (ensure-transaction [this]))

(defrecord LevelTransaction [db options]
  Transaction
  Storage
  (store [this id data]
    (assoc-in this [:cache id] (to-db data)))
  (delete [this id]
    (assoc-in this [:cache id] :deleted))
  (get-integer [this id]
    (from-db-int (.get-blob this id)))
  (get-string [this id]
    (from-db-str (.get-blob this id)))
  (get-blob [this id]
    (let [cached (get-in this [:cache id])]
      (if (= cached :deleted) nil
        (or 
          cached
          (.get db (to-db id) options)))))
  (close [this]
    (.close (.snapshot options)))
  (ensure-transaction [this] this)
  (get-iterator [this] (.iterator db))
  (commit [this]
    (with-open [batch (.createWriteBatch db)]
      (doseq [k (map #(vector  (first %) (second %)) (get this :cache))]
        (let [id (k 0)
              value (k 1)]
          (if (= value :deleted)
            (.delete db (to-db id))
            (.put db (to-db id) value))))
      (.write db batch)) nil))


(defrecord LevelStorage [db]
  Storage
  (close [this] (.close db) nil) 
  (ensure-transaction [this] 
    (let [options (ReadOptions.)
          snapshot (.getSnapshot db)]
      (.snapshot options snapshot)
      (LevelTransaction. db options))))

(defn create-db [file]
  (let [options (Options.)]
    (.createIfMissing options true)
    (.open (JniDBFactory/factory) (File. file) options)))

(defn create-storage [file]
  (LevelStorage. (create-db file)))

