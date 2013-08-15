(ns cravendb.documents
  (:use cravendb.leveldb))

(defn is-etag-docs-key [k]
  (.startsWith k "etag-docs-"))

(defn load [db id] 
  (from-db-str (safe-get db (to-db id))))

(defn delete [db id]
  (.delete db (to-db id)))

(defn last-etag [db]
  (from-db-int (safe-get db (to-db "last-etag"))))

(defn get-etag [db doc-id]
  (from-db-int (safe-get db (to-db (str "doc-etags-" doc-id)))))

(defn written-since-etag [db etag cb]
  (with-open [iterator (.iterator db)]
    (.seek iterator (to-db (str "etag-docs-" etag)))
      (cb (rest (read-all-matching iterator is-etag-docs-key)))))

(defn store [db id document] 
  (write-batch db (fn [batch]
    (let [etag (inc (last-etag db))]
      (.put batch (to-db id) (to-db document))
      (.put batch (to-db "last-etag") (to-db etag))
      (.put batch (to-db (str "etag-docs-" etag)) (to-db id))
      (.put batch (to-db (str "doc-etags-" id)) (to-db etag))))))

