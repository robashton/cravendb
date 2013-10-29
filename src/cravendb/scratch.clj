(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.data.codec.base64])
  (:require [cravendb.vclock :as v]
            [cravendb.documents :as docs]
            [clojurewerkz.vclock.core :as vclock]            
            [clojure.edn :as edn]
            [cravendb.database :as db]
            [cravendb.storage :as s]
            [me.raynes.fs :as fs]
            [cravendb.client :as client]))

(defn start []
  (def instance (db/create "testdb")))

(defn stop []
  (.close instance))

(defn restart []
  (stop)
  (fs/delete-dir "testdb")
  (start))

#_ (start)
#_ (stop)
#_ (restart)

#_ (def tx (assoc (s/ensure-transaction (:storage instance)) :e-id "1"
                  :base-vclock (:base-vclock instance)
                  :server-id "bob"))

#_ (db/check-document-write tx "doc-1"  {}
                            (fn [metadata]
                              (println "success" metadata))
                            (fn [metadata] 
                              (println "failure"  metadata)))
