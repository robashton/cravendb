(ns cravendb.vclock
  (:refer-clojure :exclude [next] )
  (:require [clojurewerkz.vclock.core :as vclock]
            [clojure.edn :as edn]
            [clojure.data.codec.base64 :refer [encode decode]] 
            ))

(defn new []
  (vclock-to-string (vclock/fresh)))

(defn vclock-to-string [clock]
  (String. (encode (.getBytes (pr-str clock)))))

(defn read-vclock [in]
  (vclock/entry (:counter in) (:timestamp in)))

(defn string-to-vclock [in]
  (edn/read-string 
    { :readers { 'clojurewerkz.vclock.core.VClockEntry read-vclock}}
    (String. (decode (.getBytes in)))))

(defn descends? [parent child]
  (vclock/descends? (string-to-vclock parent) (string-to-vclock child)))

(defn next [e-id base supplied]
  (vclock-to-string
    (if supplied
      (vclock/increment (string-to-vclock supplied) e-id)
      (vclock/increment (string-to-vclock base) e-id))))
