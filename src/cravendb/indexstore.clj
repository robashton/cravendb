(ns cravendb.indexstore
  (:require [cravendb.core]
            [cravendb.storage :as storage]
            [cravendb.documents :as docs])
  (:import (java.io File File))
  (use [cravendb.core]))

(def index-prefix "index-")
(def index-last-etag-prefix "indexlastetag-")

(defn set-last-indexed-etag-for-index [tx id etag]
  (.store tx (str index-last-etag-prefix id) etag))

(defn get-last-indexed-etag-for-index [tx id]
  (.get-string tx (str index-last-etag-prefix id)))

(defn index-doc-id [id]
  (str index-prefix id))

(defn put-index [tx index]
  (-> tx 
    (docs/store-document (index-doc-id (index :id)) (pr-str index))
    (set-last-indexed-etag-for-index (index :id) (zero-etag))))

(defn load-index [tx id]
  (let [doc (docs/load-document tx (index-doc-id id))]
    (if doc (read-string doc) nil)))

(defn iterate-indexes [iter]
  (docs/iterate-documents-prefixed-with iter index-prefix))

(defn delete-index [tx id]
  (docs/delete-document (index-doc-id id)))
