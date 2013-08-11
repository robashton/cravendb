(ns cravendb.documents)
(import 'org.iq80.leveldb.Options)
(import 'org.fusesource.leveldbjni.JniDBFactory)
(import 'java.io.File)
(import 'java.nio.ByteBuffer)

(defprotocol DocumentStorage
  "A place to store documents"
  (-put [this id document] "Puts a document into the store")
  (-get [this id] "Gets a document from the store")
  (-delete [this id] "Deletes a document from the store")
  (close [this] "Closes the storage"))

(defprotocol EtagIndexes
  "Secondary indexing by etag"
  (next-etag [this])
  (get-etag [this doc-id]))

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

(defn write-batch [db tx]
  (let [batch (.createWriteBatch db)]
    (try
      (tx batch)
      (.write db batch)
      (finally
        (.close batch)))))

(defn safe-get [db k]
  (try
    (.get db k)
    (catch Exception e nil)))

(defrecord LevelDocuments [db]
  DocumentStorage
  EtagIndexes
  (-put [this id document] 
    (write-batch db (fn [batch]
      (.put batch (to-db id) (to-db document))
      (let [etag (inc (.next-etag this))]
        (.put batch (to-db "next-etag") (to-db etag))
        (.put batch (to-db (str "doc-etags-" id)) (to-db etag))))))
  (-get [this id] 
    (from-db-str (safe-get db (to-db id))))
  (-delete [this id]
    (.delete db (to-db id)))
  (close [this] 
    (.close db))
  (next-etag [this]
    (from-db-int (safe-get db (to-db "next-etag"))))
  (get-etag [this doc-id]
    (from-db-int (safe-get db (to-db (str "doc-etags-" doc-id))))))

(defn opendb [file]
  (let [options (Options.)]
    (.createIfMissing options true)
      (.open (JniDBFactory/factory) (File. file) options)))

(defn db [file]
  (let [db (opendb file)]
    (LevelDocuments. db)))
