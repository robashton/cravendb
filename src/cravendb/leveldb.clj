(ns cravendb.leveldb)
(import 'org.iq80.leveldb.Options)
(import 'org.iq80.leveldb.DBIterator)
(import 'org.fusesource.leveldbjni.JniDBFactory)
(import 'java.io.File)
(import 'java.nio.ByteBuffer)

(defn open-db [file]
  (let [options (Options.)]
    (.createIfMissing options true)
      (.open (JniDBFactory/factory) (File. file) options)))

(defn close-db [db] 
  (.close db))

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
