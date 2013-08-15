(ns cravendb.indexes)

(defn index-key [id] (str "index-" id))
(defn index-id-key [id] (str "indexid-" id))

(defn last-index-id [docs]
  (int (.load docs "last-index-id")))
  
(defn id-for-index [docs index-name]
  (.load docs (index-id-key index-name)))

(defn store-map-index [docs id index]
  (.store docs (index-key id) index))

(defn delete-map-index [docs id]
  (.delete docs (index-key id)))

(defn load-map-index [docs id]
  (.load docs (index-key id)))

(def some-index { :map (fn [doc] (:title doc ))})
