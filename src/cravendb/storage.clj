(ns cravendb.storage
  (require [clojure.core.incubator :refer [dissoc-in]]))

(import 'org.iq80.leveldb.Options)
(import 'org.iq80.leveldb.ReadOptions)
(import 'org.iq80.leveldb.DBIterator)
(import 'org.fusesource.leveldbjni.JniDBFactory)
(import 'java.io.File)
(import 'java.nio.ByteBuffer)

#_ ;; Low level methods over the top of LevelDB

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
  (store-blob [this id data])
  (delete-blob [this id])
  (get-blob [this id])
  (commit [this]))

(defprotocol Storage 
  (close [this]) 
  (ensure-transaction [this]))

(defrecord LevelTransaction [db options]
  Transaction
  Storage
  (store-blob [this id data]
    (-> this
      (dissoc-in [:records-to-remove id])
      (assoc-in [:records-to-put id] data)))
  (delete-blob [this id]
    (-> this
      (dissoc-in [:records-to-put id])
      (assoc-in this [:records-to-remove id] true)))
  (get-blob [this id]
    (if (get-in this [:records-to-remove id])
      nil
      (or
        (get-in this [:records-to-put id])
        (from-db-str (.get db (to-db id) options)))))
  (close [this]
    (.close options))
  (ensure-transaction [this] this)
  (commit [this]))

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

#_ ;; Should not give me the blob it it doesn't exist
(with-open [db (create-db "test")]
  (let [storage (LevelStorage. db)
        tran (.ensure-transaction storage)]
    (.put db (to-db "1") (to-db "hello world")) 
    (.get-blob tran "1")))

#_ ;; Should give me the blob
(with-open [db (create-db "test")]
  (let [storage (LevelStorage. db) ]
    (.put db (to-db "1") (to-db "hello world")) 
    (let [tran (.ensure-transaction storage)]
     (.get-blob tran "1"))))

#_ ;; Basic levelDB operations

(with-open [db (create-db "test")]
  (.put db (to-db "1") (to-db "hello world")))

(with-open [db (create-db "test")]
  (from-db-str (.get db (to-db "1"))))

(with-open [db (create-db "test")]
  (.delete db (to-db "1")))

#_ ;; Having a play innit

(defn create-test-transaction []
 (LevelTransaction.))

;; Putting and retrieving an object
#_(-> (create-test-transaction)
    (.store-blob "1" "hello")
    (.get-blob "1"))

;; Deleting an object that exists
#_(-> (create-test-transaction)
    (.store-blob "1" "hello")
    (.delete-blob "1")
    (.get-blob "1"))
;;
;; Deleting an object that exists then re-creating it
#_(-> (create-test-transaction)
    (.store-blob "1" "hello")
    (.delete-blob "1")
    (.store-blob "1" "hello again")
    (.get-blob "1"))
