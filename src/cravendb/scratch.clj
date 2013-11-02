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


;; Bulk operations need to check history for conflicts
;; Replication needs to check history for conflicts
;; 

#_ (def tx (assoc (s/ensure-transaction (:storage instance))
              :e-id "root-1" 
               :base-vclock (:base-vclock instance)
               :last-synctag (:last-synctag instance)
               :server-id "root"))

#_ (pprint (r/replicate-into tx [
                      { :id "doc-1" :doc { :foo "bar" } :metadata { :synctag (integer-to-synctag 0)}}
                      { :id "doc-2" :doc { :foo "bar" } :metadata { :synctag (integer-to-synctag 1)}}
                      { :id "doc-3" :doc { :foo "bar" } :metadata { :synctag (integer-to-synctag 2)}}
                      { :id "doc-4" :doc { :foo "bar" } :metadata { :synctag (integer-to-synctag 3)}}
                      { :id "doc-5" :doc { :foo "bar" } :metadata { :synctag (integer-to-synctag 4)}}
                      ]))


;; Conflict scenarios
;; Document doesn't exist yet -> write away!
#_ (r/conflict-status
  nil (v/next "1" (v/new)))

;; Document exists, history is in the past -> write away!
#_ (r/conflict-status
  (v/next "1" (v/new)) (v/next "1" (v/next "1" (v/new))))

;; Document exists, history is in the future -> drop it!
#_ (r/conflict-status
    (v/next "1" (v/next "1" (v/new))) (v/next "1" (v/new)))

;; Document exists, history is the same -> drop it
#_ (r/conflict-status
     (v/next "1" (v/new)) (v/next "1" (v/new)))

;; Document exists, history is conflicting -> conflict
#_ (r/conflict-status
    (v/next "1" (v/next "1" (v/new))) (v/next "2" (v/next "1" (v/new))))

;; If there is no document specified, it's a delete
;; If there is a document specifeid, it's a write
#_ (r/action-for { :doc "blah"})
#_ (r/action-for {})

;; Okay, so if conflict we should write a conflict
;; If write, we should the write the document
;; If skip, we should return the un-modified 

;; Can I replicate here without too much faff?
;; Oh, I've done most of the code but I'm not assigning new synctags on write
;; I'll need to do that if I want indexing to work on written servers
;; or the daisy chaining of replication destinations

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



;; So theoretically if I run these then there should be no conflicts
;; because replication would skip un-recognised documents
;; Synctags will be screwed though
#_ (r/pump-replication (:storage dest) "http://localhost:8090")
#_ (r/pump-replication (:storage source) "http://localhost:8091")
#_ (r/last-replicated-synctag (:storage dest) "http://localhost:8090")
#_ (r/last-replicated-synctag (:storage source) "http://localhost:8091")
#_ (docs/conflicts (:storage dest))
#_ (docs/conflicts (:storage source))

;; So, let's stick a document in source, get it into dest, then run the replication to prove no side effects
#_ (db/put-document source "doc-1" { :foo "bar"})
;; Great, two way replication seemed fairly sound

;; How about sticking two different documents, one in each server
#_ (db/put-document source "doc-2" { :foo "bar"})
#_ (db/put-document dest "doc-3" { :foo "bar"})

;; How about when we cause a conflict by writing the same thing to different servers
;; Hmmm, this shouldn't do this
#_ (db/put-document source "doc-4" { :foo "source" })
#_ (db/put-document dest "doc-4" { :foo "dest" })

#_ (db/load-document source "doc-1")
#_ (db/load-document dest "doc-1")

#_ (db/load-document source "doc-2")
#_ (db/load-document dest "doc-2")

#_ (db/load-document source "doc-3")
#_ (db/load-document dest "doc-3")

#_ (db/load-document source "doc-4")
#_ (db/load-document dest "doc-4")


;; How about when we cause a conflict by modifying something on two different servers

;; What about when we have three servers? 
;; We need to make sure we're not writing in a big-ass loop
;; Theoretically the history of an object should prevent that

;; We pass the above, then we're largely sane and we can write some tests
;; The above examples all assume we're writing to the db without specifying history
;; if we do specify history then that needs to work too (I believe that's covered elsewhere though
