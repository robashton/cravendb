(ns cravendb.documents
  (use [cravendb.storage]
       [cravendb.core]))

(def etags-to-docs-prefix "etags-to-docs-")
(def docs-to-etags-prefix "docs-to-etags-")
(def document-prefix "doc-")
(def last-etag-key "last-etag")

(defn is-document-key [k]
  (.startsWith k document-prefix))

(defn is-document-key-prefixed-with [prefix entry]
  (.startsWith (entry :k) (str document-prefix prefix)))

(defn is-etags-to-docs-key [k]
  (.startsWith k etags-to-docs-prefix))

(defn is-etag-docs-entry [m]
  (is-etags-to-docs-key (m :k)))

(defn last-etag [db]
  (or (.get-string db last-etag-key) (zero-etag)))

(defn etag-for-doc [db doc-id]
  (.get-string db (str docs-to-etags-prefix doc-id)))

(defn store-document [db id document] 
  (let [etag (next-etag (last-etag db))]
    (-> db
      (.store (str document-prefix id) document)
      (.store last-etag-key etag)
      (.store (str etags-to-docs-prefix etag) id)
      (.store (str docs-to-etags-prefix id) etag))))


(defn load-document [session id] 
  (.get-string session (str document-prefix id)))

(defn query [session opts] 
  (.get-string session (str document-prefix id)))

(defn delete-document [session id]
  (.delete session (str document-prefix id)))

(defn iterate-documents-prefixed-with [iter prefix]
  (.seek iter (to-db (str document-prefix prefix)))
  (->> 
    (iterator-seq iter)
    (map expand-iterator-str)
    (take-while (partial is-document-key-prefixed-with prefix))
    (map extract-value-from-expanded-iterator)) )

(defn iterate-etags-after [iter etag]
  (.seek iter (to-db (str etags-to-docs-prefix (next-etag etag))))
  (->> 
    (iterator-seq iter)
    (map expand-iterator-str)
    (take-while is-etag-docs-entry)
    (map extract-value-from-expanded-iterator)
    (distinct)))
