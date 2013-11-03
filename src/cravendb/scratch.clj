(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.data.codec.base64])
  (:require [cravendb.vclock :as v]
            [cravendb.documents :as docs]
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
  (def source (db/create "testdb_source" :server-id "src"))
  (def dest (db/create "testdb_dest" :server-id "dest"))
  (def server-source (run-server (http/create-http-server source) { :port 8090}))
  (def server-dest (run-server (http/create-http-server dest) {:port 8091})))

(defn stop []
  (server-source)
  (server-dest)
  (.close source)
  (.close dest)
  (fs/delete-dir "testdb_source")
  (fs/delete-dir "testdb_dest") )

(defn restart []
  (stop)
  (start))

#_ (start)
#_ (stop)
#_ (restart)

(defn store-task [state]
  
  )

(defn delete-task [state]
  
  )

(defn delete-index-data [instance state]

  )

(defn run-next-task [instance]
  
  )

;; Should probably look at doing this as a macro
(deftask "delete-index-data" delete-index-data)
