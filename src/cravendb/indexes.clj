(ns cravendb.indexes
  (:require [cravendb.core]
            [cravendb.storage :as storage]
            [cravendb.documents :as docs])
  (use [cravendb.core]))

(def index-prefix "index-")

(defn index-doc-id [id]
  (str index-prefix id))

(defn put-index [tx id index]
  (docs/store-document tx (index-doc-id id) (pr-str index)))

(defn load-index [tx id]
  (read-string (docs/load-document tx (index-doc-id id))))

(defn iterate-indexes [iter]
  (docs/iterate-documents-prefixed-with iter index-prefix))

(defn delete-index [tx id]
  (docs/delete-document (index-doc-id id)))

(defn compile-index [index]
  (assoc index :map
    (load-string (index :map))))

(defn load-compiled-indexes [tx]
  (with-open [iter (.get-iterator tx)]
    (doall (map (comp compile-index read-string) (iterate-indexes iter)))))
