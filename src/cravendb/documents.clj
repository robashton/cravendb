(ns cravendb.documents
  (use [cravendb.storage]
       [cravendb.core]))

(defn is-etag-docs-key [k]
  (.startsWith k "etag-docs-"))

(defn is-etag-docs-entry [m]
  (is-etag-docs-key (m :k)))

(defn last-etag [db]
  (.get-integer db "last-etag"))

(defn etag-for-doc [db doc-id]
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
  (.get-string session (str "doc-" id)))

(defn delete-document [session id]
  (.delete session (str "doc-" id)))

(defn delete-document [session id]
  (.delete session (str "doc-" id)))


(defn iterate-etags-after [iter etag]
  (.seek iter (to-db (str "etag-docs-" (inc etag))))
  (->> 
    (iterator-seq iter)
    (map expand-iterator-str)
    (take-while is-etag-docs-entry)
    (map extract-value-from-expanded-iterator)
    (distinct)))

#_ (def storage (create-storage "test")) 
#_ (.close storage)
#_ (def tx (.ensure-transaction storage))
#_ (store-document tx "1" "document")

