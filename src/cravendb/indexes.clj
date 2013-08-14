(ns cravendb.indexes)


(defn index-id [id] (str "index-" id))

(defn store-map-index [docs id index]
  (.store docs (index-id id) index))
(defn delete-map-index [docs id]
  (.delete docs (index-id id)))
(defn load-map-index [docs id]
  (.load docs (index-id id)))
