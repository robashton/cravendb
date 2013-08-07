(ns cravendb.documents)
(import 'org.iq80.leveldb.Options)
(import 'org.fusesource.leveldbjni.JniDBFactory)
(import 'java.io.File)

(defprotocol DocumentStorage
  "A place to store documents"
  (-put [this id document] "Puts a document into the store")
  (-get [this id] "Gets a document from the store")
  (-delete [this id] "Deletes a document from the store")
  (close [this] "Closes the storage"))

(defprotocol EtagIndexes
  "Secondary indexing by etag"
  (next-etag [this]))

(defn to-db [input]
  (.getBytes input "UTF-8"))

(defn from-db [input]
  (if (= input nil)
    nil
    (String. input "UTF-8")))

(defrecord LevelDocuments [db]
  DocumentStorage
  EtagIndexes
  (-put [this id document] 
    (.put db (to-db id) (to-db document)))
  (-get [this id] 
    (try
      (from-db (.get db (to-db id)))
      (catch Exception e nil)))
  (-delete [this id]
    (.delete db (to-db id)))
  (close [this] 
    (.close db))
  (next-etag [this] 0))

(defn opendb [file]
  (let [options (Options.)]
    (.createIfMissing options true)
      (.open (JniDBFactory/factory) (File. file) options)))

(defn db [file]
  (let [db (opendb file)]
    (LevelDocuments. db)))
