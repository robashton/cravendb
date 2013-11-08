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

#_(with-open [stream (db/stream-into "key")] 
    (binding [*out* (clojure.java.io/writer stream)]
      (pr 2)
      (pr "hello")
      (pr { :hello "world"})
      (.flush *out*)))

#_ (let [*out* ] 
    
    )
