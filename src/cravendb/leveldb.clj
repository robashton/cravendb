(ns cravendb.leveldb)
(import 'org.iq80.leveldb.Options)
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

(defn open-db [file]
  (let [options (Options.)]
    (.createIfMissing options true)
      (.open (JniDBFactory/factory) (File. file) options)))

(defn close-db [db] 
  (.close db))


(defprotocol KeyValueStore
  "A generic protocol on top of any key value store"
  (store [this id data])
  (get-string [this id])
  (get-integer [this id])
  (delete [this id])
  (get-iterator [this]))

(defrecord LevelOperations [batch]
  KeyValueStore
  (store [this id data]
    (.put batch (to-db id) (to-db data))
    this) "Mutate our inner collection"
  (get-string [this id]
    (println "Getting " id)
    (from-db-str (safe-get batch (to-db id))))
  (get-integer [this id]
    (from-db-int (safe-get batch (to-db id))))
  (delete [this id]
    (.delete batch (to-db id))
    this) "Mutate our innner collection"
  (get-iterator [this]
    (.iterator batch)))

(defn read-all-matching [iterator key-predicate]
  (if
    (->>
      (.peekNext iterator)
      .getKey
      from-db-str
      key-predicate
      (and (.hasNext iterator)))
    (do
      (->
        (.next iterator)
        .getValue
        from-db-str
        (cons (lazy-seq (read-all-matching iterator key-predicate)))))
    ()))

(defn perform-over [db tx]
  (with-open [batch (.createWriteBatch db)]
    (tx (LevelOperations. batch))
    (.write db batch)))
