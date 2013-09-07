(ns cravendb.storage
  (require [clojure.core.incubator :refer [dissoc-in]]))

(import 'org.iq80.leveldb.Options)
(import 'org.iq80.leveldb.DBIterator)
(import 'org.fusesource.leveldbjni.JniDBFactory)
(import 'java.io.File)
(import 'java.nio.ByteBuffer)

(defprotocol Storage
  (store-blob [this id data])
  (delete-blob [this id])
  (get-blob [this id]))

(defrecord LevelStorage []
  Storage
  (store-blob [this id data]
    (-> this
      (dissoc-in [:records-to-remove id])
      (assoc-in [:records-to-put id] data)))
  (delete-blob [this id]
    (assoc-in this [:records-to-remove id] true))
  (get-blob [this id]
    (if (get-in this [:records-to-remove id])
      nil
      (get-in this [:records-to-put id]))))

#_ (def mymap {})
#_ (get (assoc mymap "id" 2) )

(defn create-storage []
  (LevelStorage. {} {}))

#_ ;; Having a play innit

;; Putting and retrieving an object
#_(-> (create-storage)
    (.store-blob "1" "hello")
    (.get-blob "1"))

;; Deleting an object that exists
#_(-> (create-storage)
    (.store-blob "1" "hello")
    (.delete-blob "1")
    (.get-blob "1"))
;;
;; Deleting an object that exists then re-creating it
#_(-> (create-storage)
    (.store-blob "1" "hello")
    (.delete-blob "1")
    (.store-blob "1" "hello again")
    (.get-blob "1"))



