(ns cravendb.indexes
  (:require [cravendb.documents :as docs]))

(defn index-key [id] (str "index-" id))
(defn index-id-key [id] (str "indexid-" id))

(defn last-index-id [ops]
  (.get-integer ops "last-index-id"))
  
(defn id-for-index [ops index-name]
  (.get-integer ops (index-id-key index-name)))

(defn store-map-index [ops id index]
  (docs/store-document ops (index-key id) index))

(defn delete-map-index [ops id]
  (docs/delete-document ops (index-key id)))

(defn load-map-index [ops id]
  (docs/load-document ops (index-key id)))

(def some-index { :map (fn [doc] (:title doc ))})
