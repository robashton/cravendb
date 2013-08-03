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

(defn to-db [input]
  (println "todb" input)
  (.getBytes input "UTF-8"))

(defn from-db [input]
  (println "fromdb" (String. input "UTF-8"))
  (String. input "UTF-8"))

(defrecord LevelDocuments [db]
  DocumentStorage
  (-put [this id document] 
    (.put db (to-db id) (to-db document)))
  (-get [this id] 
    (from-db (.get db (to-db id))))
  (-delete [this id] )
  (close [this] (.close db)))

(defn opendb [file]
  (let [options (Options.)]
    (.createIfMissing options true)
      (.open (JniDBFactory/factory) (File. file) options)))

(defn db [file]
  (LevelDocuments. (opendb file)))

