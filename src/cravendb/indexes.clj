(ns cravendb.indexes
  (:require [cravendb.core]
            [cravendb.storage :as storage]
            [cravendb.documents :as docs])
  (use [cravendb.core]))

(def index-prefix "index-")

(defn index-doc-id [id]
  (str index-prefix id))

(defn put-index [tx id defn]
  (docs/store-document (index-doc-id id) (pr-str defn)))

(defn load-index [tx id]
  (read-string (docs/load-document (index-doc-id id))))

(defn iterate-indexes [iter]
  (docs/iterate-documents-prefixed-with iter index-prefix))

(defn delete-index [tx id]
  (docs/delete-document (index-doc-id id)))
