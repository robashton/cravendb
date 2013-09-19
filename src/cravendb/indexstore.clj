(ns cravendb.indexstore
  (:require [cravendb.core]
            [cravendb.storage :as storage]
            [cravendb.documents :as docs])
  (:import (java.io File File))
  (use [cravendb.core]))

(def index-prefix "index-")


(defn index-doc-id [id]
  (str index-prefix id))

(defn put-index [tx index]
  (docs/store-document tx (index-doc-id (index :id)) (pr-str index)))

(defn load-index [tx id]
  (let [doc (docs/load-document tx (index-doc-id id))]
    (if doc (read-string doc) nil)))

(defn iterate-indexes [iter]
  (docs/iterate-documents-prefixed-with iter index-prefix))

(defn delete-index [tx id]
  (docs/delete-document (index-doc-id id)))
