(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core])

  (:require [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.http :as http]
            [cravendb.client :as c] 
            [cravendb.database :as db]
            [me.raynes.fs :as fs]
            [cravendb.storage :as s]
            [cravendb.documents :as docs]
            [cravendb.core :refer [zero-etag]]
            [cravendb.replication :as replication]
            [clojure.tools.logging :refer [info error debug]]
            ))

(def instance nil)
(def destinstance nil)

;;
;; What I really want is a stream of the whole documents and their metadata
;; In the order in which they were written from a specific e-tag
;; What I'd probably do is keep documents in memory once written
;; because they'd need to be hit by both indexing and replication
;;
;; What I also probably want to do is keep a list of etags/ids written
;; and generate the stream information from that rather than hitting the database
;; ideally, consuming the stream shouldn't involve disk IO
;; We can probably even push the stream as an edn stream using edn/read-string


(defn start-master []
  (def instance (db/create "testdb")) 
    (def server 
     (run-jetty 
      (http/create-http-server instance) 
      { :port (Integer/parseInt (or (System/getenv "PORT") "8080")) :join? false}))

  (db/bulk instance
      (map (fn [i]
      {
        :operation :docs-put
        :id (str "docs-" i)
        :document { :whatever (str "Trolololol" i)} 
        }) (range 0 5000))))

(defn start-slave []
  (def destinstance (db/create "testdb2"))
  (def replication-handle (replication/create destinstance "http://localhost:8080"))
  (def destserver 
    (run-jetty 
    (http/create-http-server destinstance) 
    { :port (Integer/parseInt (or (System/getenv "PORT") "8081")) :join? false})))

(defn stop-master []
   (.stop server)   
   (.close instance))

(defn stop-slave []
  (.stop destserver) 
  (.close destinstance)
  (.close replication-handle))

(defn start-slave-replication []
  (replication/start replication-handle)
  )

(defn stop-slave-replication []
  (replication/stop replication-handle))

(defn start []
  (start-master)
  (start-slave))

(defn stop []
  (stop-master)
  (stop-slave))

(defn restart []
  (stop)
  (fs/delete-dir "testdb") 
  (fs/delete-dir "testdb2") 
  (start))


#_ (start)
#_ (restart)
 

;; Master -> Slave Happenings (easy)
;; NOTE we need the bulk operation and the writing of "last etag" to happen in a tx
;; NOTE: We're not taking into account indexing as part of this if we bypass database
;; Maybe indexing needs to be reading off a queue
;; Or maybe indexing needs to have a queue as an extra

;; On starting up the second server, we should see the documents that were written to the first
;; They should have the same etags as the first
;; They will anyway because it's a slave so it's "magic"

#_ (count (c/stream-seq "http://localhost:8081"))
#_ (count (c/stream-seq "http://localhost:8080"))
#_ (first (c/stream-seq "http://localhost:8081"))
#_ (first (c/stream-seq "http://localhost:8080"))

;; On adding documents to the first server, they should appear on the second
;; They should have the same etags as the first

#_ (c/put-document "http://localhost:8080" "new-doc" { :hello "world"})
#_ (c/get-document "http://localhost:8080" "new-doc")
#_ (c/get-document "http://localhost:8081" "new-doc")

;; On updating documents in the first server, they should update in the second
;; They should have updated etags the same as the first

#_ (c/put-document "http://localhost:8080" "great-doc" { :hello "bill"})
#_ (c/put-document "http://localhost:8080" "great-doc" { :hello "bob"})
#_ (c/get-document "http://localhost:8081" "great-doc")


;; The slave stores where it is currently caught up to in storage somewhere
;; It also stores the number of documents it has received from each node
;; This will be useful for testing and feedback

#_ (replication-status (:storage destinstance) "http://localhost:8080")

;; If I shut down the slave, it should continue from where it left off

;; If I shut down the master, the slave should gracefully await instruction

;; If I delete a document in master, it should be deleted in slave

#_ (c/delete-document "http://localhost:8080" "great-doc")
#_ (c/get-document "http://localhost:8081" "great-doc")

;; Master -> Master Happenings (Currently impossible)
;; Will ignore this until we have tests for master -> slave in place

;; On writing a document to the first server, it should appear in the second
;; It should not be replicated back to the other server (how?)
;; We could just provide the list in ordered e-tag terms, and we can do a look-up
;; And prevent replication

;; What happens when writing 
;; doc-1 -> a1 -> b1
;; doc-1 -> b2 -> a2
;; We should know that a2 is a descendent of a1
;; We can do that by using vector-clocks
;; We could also achieve it by storing an audit history (this would get expensive)
