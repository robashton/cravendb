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
            ))

(defn start []
    (def instance 
      (do
        (fs/delete-dir "testdb")
        (db/create "testdb"))) 
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

(defn stop []
   (.stop server)   
   (.close instance))


(defn restart []
  (stop)
  (start))


#_ (start)
#_ (restart)
 
;; What I really want is a stream of the whole documents and their metadata
;; In the order in which they were written from a specific e-tag
;; What I'd probably do is keep documents in memory once written
;; because they'd need to be hit by both indexing and replication
;;
;; What I also probably want to do is keep a list of etags/ids written
;; and generate the stream information from that rather than hitting the database
;; ideally, consuming the stream shouldn't involve disk IO


;; Can possibly do this with core.async
(defn synchronise [input dest]
  (doseq [d (take 100 input)]

    ))

#_ (with-open [iter (s/get-iterator (:storage instance))] 
     (doall (map expand-document 
          (docs/iterate-etags-after iter (zero-etag)))))


#_ (c/stream "http://localhost:8080")
