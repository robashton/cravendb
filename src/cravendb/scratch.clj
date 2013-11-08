(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.tools.logging :only (info debug error)]
        [clojure.core.async])
  (:import (java.io File File PushbackReader IOException FileNotFoundException ))
  (:require [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            ))


(defn to-db [v]
  (with-open [stream (java.io.ByteArrayOutputStream.)] 
    (binding [*out* (clojure.java.io/writer stream)]
      (pr v)
      (.flush *out*))
    (.toByteArray stream)))

(defn from-db [v]
  (with-open [reader (java.io.PushbackReader.
                          (clojure.java.io/reader 
                            (java.io.ByteArrayInputStream. v)))]
    (edn/read reader)))


(from-db (to-db 2))
(from-db (to-db "2"))
(from-db (to-db {:hello "world"}))


#_ (with-open [stream (java.io.ByteArrayOutputStream.)] 
    (binding [*out* (clojure.java.io/writer stream)]
      (pr {:hello "world"})
      (pr {:hello "world again"})
      (.flush *out*))

     (with-open [reader (java.io.PushbackReader.
                          (clojure.java.io/reader 
                            (java.io.ByteArrayInputStream. 
                              (.toByteArray stream))))]

       ) )

