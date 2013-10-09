(ns cravendb.indexstore
  (:require [cravendb.core]
            [cravendb.storage :as s]
            [cravendb.documents :as docs])
  (:import (java.io File File))
  (:use [cravendb.core]))

(def index-prefix "index-")
(def index-error-prefix "indexerror-")
(def index-last-etag-prefix "indexlastetag-")

(defn set-last-indexed-etag-for-index [tx id etag]
  (s/store tx (str index-last-etag-prefix id) etag))

(defn get-last-indexed-etag-for-index [tx id]
  (or (s/get-string tx (str index-last-etag-prefix id))
      (zero-etag)))

(defn index-doc-id [id]
  (str index-prefix id))

(defn index-error-id [id]
  (str index-error-prefix id))

(defn put-index [tx index]
  (-> tx 
    (docs/store-document (index-doc-id (index :id)) (pr-str index))
    (set-last-indexed-etag-for-index (index :id) (zero-etag))))

(defn mark-failed [tx index-id info]
  (s/store tx (index-error-id index-id) (pr-str info)))

(defn is-failed [tx index-id]
  (boolean (s/get-string tx (index-error-id index-id))))

(defn errors [tx index-id]
  (s/get-string (index-error-id index-id)))

(defn reset-index [tx index-id]
  (-> tx
    (s/delete (index-error-id index-id))
    (set-last-indexed-etag-for-index index-id (zero-etag))))

(defn load-index [tx id]
  (let [doc (docs/load-document tx (index-doc-id id))]
    (if doc (read-string doc) nil)))

(defn etag-for-index [tx id]
  (docs/etag-for-doc tx (index-doc-id id)))

(defn iterate-indexes [iter]
  (docs/iterate-documents-prefixed-with iter index-prefix))

(defn delete-index [tx id]
  (docs/delete-document tx (index-doc-id id)))
