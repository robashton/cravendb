(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core])

  (:require [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.http :as http]
            [cravendb.client :as c] 
            [cravendb.database :as db]
            [me.raynes.fs :as fs]
            ))

#_ (def instance 
  (do
    (fs/delete-dir "testdb")
    (db/create "testdb")))

#_ (def server 
     (run-jetty 
      (http/create-http-server instance) 
      { :port (Integer/parseInt (or (System/getenv "PORT") "8080")) :join? false}))

#_ (.stop server)
#_ (.close instance)

#_ (db/load-document instance "1")

#_ (c/put-document "http://localhost:8080" "1" { :foo "bar"})
#_ (c/get-document "http://localhost:8080" "1" )


