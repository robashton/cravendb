(ns cravendb.documents
  (use [cravendb.storage]))

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
  (.get-string session (str "doc-" id)))

(defn delete-document [session id]
  (.delete session (str "doc-" id)))

(defn delete-document [session id]
  (.delete session (str "doc-" id)))

(defn documents-written-since-etag [session etag cb]
  (.read-range session (str "etag-docs" etag) is-etag-docs-key cb))

#_ (def storage (create-storage "test")) 
#_ (.close storage)
#_ (def tx (.ensure-transaction storage))
#_ (store-document tx "1" "document")

