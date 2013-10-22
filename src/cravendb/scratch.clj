(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.data.codec.base64])
  (:require [cravendb.vclock :as v]
            [clojure.edn :as edn]
            [cravendb.database :as db]
            [me.raynes.fs :as fs]
            ))

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



#_ (db/checked-history {:server-id "2"} "doc-1"nil nil) 
#_ (db/put-document instance "doc-1" {:name "bob"})
#_ (db/load-document instance "doc-1" )
#_ (db/load-document-metadata instance "doc-1")

#_ (db/put-document instance "doc-1" {:name "bob"}
   (db/load-document-metadata instance "doc-1"))


#_ (next-vclock "1" (vclock/fresh) nil)
#_ (next-vclock "1" (vclock/fresh) 
                (vclock-to-string (vclock/increment (vclock/fresh) "2")))





























