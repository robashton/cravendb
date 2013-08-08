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
  (next-etag [this]))

"(defn to-db [input]"
  "(if (string? input)"
"    (.getBytes input \"UTF-8\"))"
  "(if (integer? input)"
  "  (.array (.putInt (.allocate ByteBuffer 4) input))))"

(defn from-db-str [input]
  (if (= input nil)
    nil
    (String. input "UTF-8")))

(defn from-db-int [input]
  (if (= input nil)
    nil
    (.getInt (.wrap ByteBuffer input))))

(defn write-batch [db tx]
  (let [batch (.createWriteBatch db)]
    (try
      (tx batch)
      (.write db batch)
      (finally
        (.close batch)))))

(defrecord LevelDocuments [db]
  DocumentStorage
  EtagIndexes
  (-put [this id document] 
    (write-batch db (fn [batch]
      (.put batch (to-db id) (to-db document)
      (.put batch (to-db "next-etag" (to-db (inc (.next-etag this)))))))))
  (-get [this id] 
    (try
      (from-db-str (.get db (to-db id)))
      (catch Exception e nil))) "what really????"
  (-delete [this id]
    (.delete db (to-db id)))
  (close [this] 
    (.close db))
  (next-etag [this]
    (.get (from-db-int (.get db (to-db "next-etag"))))))

(defn opendb [file]
  (let [options (Options.)]
    (.createIfMissing options true)
      (.open (JniDBFactory/factory) (File. file) options)))

(defn db [file]
  (let [db (opendb file)]
    (LevelDocuments. db)))
