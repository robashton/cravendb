(ns cravendb.vclock
  (:refer-clojure :exclude [next] )
  (:require [clojurewerkz.vclock.core :as vclock]
            [clojure.edn :as edn]
            [clojure.data.codec.base64 :refer [encode decode]] 
            ))

(defn vclock-to-string [clock]
  (String. (encode (.getBytes (pr-str clock)))))

(defn read-vclock [in]
  (vclock/entry (:counter in) (:timestamp in)))

(defn string-to-vclock [in]
  (edn/read-string 
    { :readers { 'clojurewerkz.vclock.core.VClockEntry read-vclock}}
    (String. (decode (.getBytes in)))))

(defn new []
  (vclock-to-string (vclock/fresh)))

(defn descends? [child parent]
  (vclock/descends?  (string-to-vclock child)  (string-to-vclock parent)))

(defn next [e-id supplied]
  (vclock-to-string
    (vclock/increment (string-to-vclock supplied) e-id)))
