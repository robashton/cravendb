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

#_ (with-test-server 
      (fn []
        (client/put-index 
          "http://localhost:9000" 
          "by_username" 
          "(fn [doc] {\"username\" (:username doc)})")
        (client/put-document 
          "http://localhost:9000" 
          "1" { :username "bob"})
        (client/put-document 
          "http://localhost:9000" 
          "2" { :username "alice"})
        (client/query 
          "http://localhost:9000" 
          { :query "(= \"username\" \"bob\")" :index "by_username" :wait true}))) 

;; I think I should rename synctags, database-specific incrementor
;; - Used for indexing location
;; - Used for replication location
;; - the "history" for an item

;; The server should assign client-ids
;; When an in-flight transaction starts
;; It should be a combination of the server id and some integer
;; I can code that up in the REPL

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


