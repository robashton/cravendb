(ns cravendb.documents)
(import 'org.iq80.leveldb.Options)
(import 'org.fusesource.leveldbjni.JniDBFactory)
(import 'java.io.File)

(defprotocol DocumentStorage
  "A place to store documents"
  (-put [id document] "Puts a document into the store")
  (-get [id] "Gets a document from the store")
  (-delete [id] "Deletes a document from the store"))

(deftype LevelDocuments [db]
  DocumentStorage
  (-put [id document]

    )
  (-get [id]

    )
  (-delete [id]

    ))

(defn opendb [file]
  (let [options (Options.)]
    (.createIfMissing options true)
    (.open (JniDBFactory/factory) (File. file) options)))

(defn scratch []
  (let [db (opendb "test")]
    (LevelDocuments. db)
    (.close db))) 

