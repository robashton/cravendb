(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.data.codec.base64])
  (:require [cravendb.vclock :as v]
            [cravendb.documents :as docs]
            [cravendb.tasks :as tasks]
            [cravendb.http :as http]
            [clojurewerkz.vclock.core :as vclock]            
            [org.httpkit.server :refer [run-server]]
            [clojure.edn :as edn]
            [cravendb.database :as db]
            [cravendb.storage :as s]
            [me.raynes.fs :as fs]
            [cravendb.client :as client]
            [cravendb.replication :as r]
            [clojure.pprint :refer [pprint]]))


(defn start []
  (def source (db/create "testdb_source" :server-id "src")))

(defn stop []
  (.close source) 
  (fs/delete-dir "testdb_source"))

(defn restart []
  (stop)
  (start))

#_ (start)
#_ (stop)
#_ (restart)



