(ns cravendb.documents
  (require [cravendb.storage]))

(defn is-etag-docs-key [k]
  (.startsWith k "etag-docs-"))

(defn last-etag [db]
  (.get-integer db "last-etag"))

(defn get-document-etag [db doc-id]
  (.get-integer db (str "doc-etags-" doc-id)))

(defn store-document [db id document] 
  (let [etag (inc (last-etag db))]
    (-> db
      (.store (str "doc-" id) document)
      (.store (str "doc-" id) document)
      (.store "last-etag" etag)
      (.store (str "etag-docs-" etag) id)
      (.store (str "doc-etags-" id) etag))))

(defn load-document [session id] 
  (.get-string ops (str "doc-" id)))

(defn delete-document [session id]
  (.delete ops (str "doc-" id)))

(defn delete-document [ops id]
  (.delete ops (str "doc-" id)))

(defn documents-written-since-etag [ops etag cb]
  (with-open [iterator (.get-iterator ops)]
    (.seek iterator (to-db (str "etag-docs-" etag)))
      (cb (rest (read-all-matching iterator is-etag-docs-key)))))
