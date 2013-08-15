(ns cravendb.indexes
  (:require [cravendb.documents :as docs]))

(defn index-key [id] (str "index-" id))
(defn index-id-key [id] (str "indexid-" id))

(defn last-index-id [docs]
  (int (docs/load docs "last-index-id")))
  
(defn id-for-index [docs index-name]
  (docs/load docs (index-id-key index-name)))

(defn store-map-index [docs id index]
  (docs/store docs (index-key id) index))

(defn delete-map-index [docs id]
  (docs/delete docs (index-key id)))

(defn load-map-index [docs id]
  (docs/load docs (index-key id)))

(def some-index { :map (fn [doc] (:title doc ))})
