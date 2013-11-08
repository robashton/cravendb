(ns cravendb.indexstore
  (:require [cravendb.core]
            [cravendb.storage :as s]
            [cravendb.documents :as docs])
  (:import (java.io File File))
  (:use [cravendb.core]
        [clojure.tools.logging :only (info debug error)]
        ))

(def index-prefix "index-")
(def index-error-prefix "indexerror-")
(def index-last-synctag-prefix "indexlastsynctag-")

(defn set-last-indexed-synctag-for-index [tx id synctag]
  (s/store tx (str index-last-synctag-prefix id) synctag))

(defn get-last-indexed-synctag-for-index [tx id]
  (or (s/get-obj tx (str index-last-synctag-prefix id))
      (zero-synctag)))

(defn index-doc-id [id]
  (str index-prefix id))

(defn index-error-id [id]
  (str index-error-prefix id))

(defn put-index [tx index synctag]
  (-> tx 
    (docs/store-document (index-doc-id (index :id)) index synctag)
    (set-last-indexed-synctag-for-index (index :id) (zero-synctag))))

(defn mark-failed [tx index-id info]
  (s/store tx (index-error-id index-id) info))

(defn is-failed [tx index-id]
  (boolean (s/get-obj tx (index-error-id index-id))))

(defn errors [tx index-id]
  (s/get-obj (index-error-id index-id)))

(defn reset-index [tx index-id]
  (-> tx
    (s/delete (index-error-id index-id))
    (set-last-indexed-synctag-for-index index-id (zero-synctag))))

(defn load-index [tx id]
  (docs/load-document tx (index-doc-id id)))

(defn synctag-for-index [tx id]
  (docs/synctag-for-doc tx (index-doc-id id)))

(defn iterate-indexes [iter]
  (docs/iterate-documents-prefixed-with iter index-prefix))

(defn delete-index [tx id synctag]
  (docs/delete-document tx (index-doc-id id) synctag))
