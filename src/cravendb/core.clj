(ns cravendb.core
  (use [cravendb.storage]))

(defn expand-iterator-str [i]
  { :k (from-db-str (.getKey i))
    :v (from-db-str (.getValue i)) })

(defn extract-value-from-expanded-iterator [m] (m :v))
