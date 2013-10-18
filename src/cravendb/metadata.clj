(ns cravendb.metadata)

(defn create [& kvs]
  (apply assoc 
    {
    } kvs))

(defn touch [metadata synctag]
  (assoc metadata :synctag synctag))

