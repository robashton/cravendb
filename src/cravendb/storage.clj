(ns cravendb.storage)
(import 'org.iq80.leveldb.Options)
(import 'org.iq80.leveldb.DBIterator)
(import 'org.fusesource.leveldbjni.JniDBFactory)
(import 'java.io.File)
(import 'java.nio.ByteBuffer)

(defprotocol Storage
  (store-blob [this id data])
  (get-blob [this id]))

(defrecord LevelStorage [records-to-remove records-to-put]
  Storage
  (store-blob [this id data]
    (assoc-in this [:records-to-put id] data))
  (get-blob [this id]
    (get records-to-put id)))

#_ (def mymap {})
#_ (get (assoc mymap "id" 2) )

(defn create-storage []
  (LevelStorage. {} {}))

#_ ;; Having a play innit

#_ (def storage (create-storage))

;; Putting and retrieving an object
#_(-> storage
    (.store-blob "1" "hello")
    (.get-blob "1"))



