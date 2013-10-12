(ns cravendb.documents
  (:use [clojure.tools.logging :only (info error debug)] 
       [cravendb.core])
  (:require [cravendb.storage :as s]))

(def etags-to-docs-prefix "etags-to-docs-")
(def docs-to-etags-prefix "docs-to-etags-")
(def document-prefix "doc-")
(def last-etag-key "last-etag")

(defn is-document-key [^String k]
  (.startsWith k document-prefix))

(defn is-document-key-prefixed-with [prefix entry]
  (.startsWith (entry :k) (str document-prefix prefix)))

(defn is-etags-to-docs-key [k]
  (.startsWith k etags-to-docs-prefix))

(defn is-etag-docs-entry [m]
  (is-etags-to-docs-key (m :k)))

(defn etag-for-doc [db doc-id]
  (s/get-string db (str docs-to-etags-prefix doc-id)))

(defn last-etag-in
  [storage]
  (or (s/get-string storage last-etag-key) (zero-etag)) )

(defn write-last-etag
  [tx last-etag]
  (s/store tx last-etag-key (integer-to-etag @last-etag)))

(defn store-document [db id document etag] 
  (-> db
    (s/store (str document-prefix id) document)
    (s/store (str etags-to-docs-prefix etag) id)
    (s/store (str docs-to-etags-prefix id) etag)))

(defn load-document [session id] 
  (s/get-string session (str document-prefix id)))

(defn delete-document [session id]
  (s/delete session (str document-prefix id)))

(defn iterate-documents-prefixed-with [iter prefix]
  (.seek iter (s/to-db (str document-prefix prefix)))
  (->> 
    (iterator-seq iter)
    (map expand-iterator-str)
    (take-while (partial is-document-key-prefixed-with prefix))
    (map extract-value-from-expanded-iterator)) )

(defn iterate-etags-after [iter etag]
  (debug "About to iterate etags after" etag)
  (.seek iter (s/to-db (str etags-to-docs-prefix (next-etag etag))))
  (->> 
    (iterator-seq iter)
    (map expand-iterator-str)
    (take-while is-etag-docs-entry)
    (map extract-value-from-expanded-iterator)
    (distinct)))
