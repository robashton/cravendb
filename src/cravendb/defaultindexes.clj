(ns cravendb.defaultindexes
  (:require [clojure.tools.logging :refer [info debug error]] ))

(defn generate-key-name [prefix k]
  (str (if prefix (str prefix "$")) k))

(defn normalize [prefix]
  (or prefix "value"))

(defn strip-document 
  ([prefix doc]
   (cond 
     (string? doc) [(normalize prefix) doc]
     (integer? doc) [(normalize prefix) doc]
     (float? doc) [(normalize prefix) doc]
     (decimal? doc) [(normalize prefix) doc]
     (or (list? doc) (seq? doc) (vector? doc)) (flatten (map #(strip-document prefix %1) doc))
     (map? doc) (for [[k v] doc]
                  (flatten (strip-document (generate-key-name prefix k) v)) )))
  ([doc] (let [result (flatten (strip-document nil doc))]
           (debug "Mapped to" result)
           result)))

(defn two-at-a-time [remaining]
     (if (empty? remaining) nil
         (cons (take 2 remaining) 
         (lazy-seq (two-at-a-time (drop 2 remaining))))))

(defn put-pairs-into-obj [output item]
  (let [k (first item)
        v (last item)
        existing (get output k) ]
    (if existing 
      (if (coll? existing)
        (assoc output k (conj existing v))
        (assoc output k [existing v]))
      (assoc output k v))))

(defn map-document [doc]
  (debug "Mapping" doc)
  (reduce put-pairs-into-obj {} (two-at-a-time (strip-document doc))) )

(defn all [] [
  { :id "default"
    :map map-document 
   }
   ])
