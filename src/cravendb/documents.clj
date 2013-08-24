(ns cravendb.documents
  (:use cravendb.leveldb))

(defn is-etag-docs-key [k]
  (.startsWith k "etag-docs-"))

(defn last-etag [ops]
  (.get-integer ops "last-etag"))

(defn get-document-etag [ops doc-id]
  (.get-integer ops (str "doc-etags-" doc-id)))

(defn store-document [ops id document] 
  (let [etag (inc (last-etag ops))]
    (-> 
      ops
      (.store (str "doc-" id) document)
      (.store (str "doc-" id) document)
      (.store "last-etag" etag)
      (.store (str "etag-docs-" etag) id)
      (.store (str "doc-etags-" id) etag))))

(defn load-document [ops id] 
  (.get-string ops (str "doc-" id)))

(defn delete-document [ops id]
  (.delete ops (str "doc-" id)))

(defn documents-written-since-etag [ops etag cb]
  (with-open [iterator (.get-iterator ops)]
    (.seek iterator (to-db (str "etag-docs-" etag)))
      (cb (rest (read-all-matching iterator is-etag-docs-key)))))


