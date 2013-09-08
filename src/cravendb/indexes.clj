(ns cravendb.indexing
  (:require [cravendb.storage :as storage]
            [cravendb.documents :as docs])
  (use [cravendb.core]))

(defn index-doc-id [id]
  (str "index-" id))

(defn put-index [tx id defn]
  (docs/store-document (index-doc-id id) (pr-str defn)))

(defn load-index [tx id]
  (read-string (docs/load-document (index-doc-id id))))

(defn reify-index [tx id]
  (load-index tx id)
  )

(defn iterate-indexes [iter etag]
  (.seek iter (to-db "index-"))
  (->> 
    (iterator-seq iter)
    (map expand-iterator-str)
    (take-while is-index-entry)
    (map extract-value-from-expanded-iterator)))

(defn delete-index [tx id]
  (docs/delete-document (index-doc-id id)))
