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
            [clojure.tools.logging :refer [info error debug]]
            ))

(defn start []
    (def instance 
      (do
        (fs/delete-dir "testdb")
        (db/create "testdb"))) 

    (def destinstance
      (do
        (fs/delete-dir "testdb2")
        (db/create "testdb2")))

    (def server 
     (run-jetty 
      (http/create-http-server instance) 
      { :port (Integer/parseInt (or (System/getenv "PORT") "8080")) :join? false}))
    
    (def destserver 
     (run-jetty 
      (http/create-http-server destinstance) 
      { :port (Integer/parseInt (or (System/getenv "PORT") "8081")) :join? false}))

    (db/bulk instance
      (map (fn [i]
      {
        :operation :docs-put
        :id (str "docs-" i)
        :document { :whatever (str "Trolololol" i)} 
        }) (range 0 5000))))

(defn stop []
   (.stop server)   
   (.stop destserver)   
   (.close instance)
   (.close destinstance))


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
;; We can probably even push the stream as an edn stream using edn/read-string


;; Can possibly do this with core.async
#_ (with-open [iter (s/get-iterator (:storage instance))] 
     (doall (map expand-document 
          (docs/iterate-etags-after iter (zero-etag)))))

#_ (c/get-document "http://localhost:8080" "docs-1")
#_ (c/put-document "http://localhost:8080" "robisamazing" { :foo "bar"})

(defn stream-sequence 
  ([url] (stream-sequence url (zero-etag)))
  ([url etag] (stream-sequence url etag (c/stream url etag)))
  ([url last-etag src]
   (if (empty? src) ()
     (let [{:keys [metadata doc] :as item} (first src)]
       (cons item (lazy-seq (stream-sequence url (:etag metadata) (rest src))))))))

(defn pump-readers [etag]
  (loop [last-etag etag
         items (stream-sequence "http://localhost:8080" etag)]
    (if (empty? items)
      (do
        (Thread/sleep 100)
        (pump-readers last-etag))
      (do
        (let [batch (take 100 items)] 
          (db/bulk 
            destinstance
            (map (fn [i] {:document (:doc i) :id (:id i) :operation :docs-put}) batch)
                   )

          (recur (get-in (last batch) [:metadata :etag]) (drop 100 items)))))))

#_ (def worker (future (pump-readers (zero-etag))))
#_ (future-cancel worker)

;; Master -> Slave Happenings (easy)

;; On starting up the second server, we should see the documents that were written to the first
;; They should have the same etags as the first
;; They will anyway because it's a slave so it's "magic"

#_ (count (stream-sequence "http://localhost:8081"))
#_ (count (stream-sequence "http://localhost:8080"))
#_ (first (stream-sequence "http://localhost:8081"))
#_ (first (stream-sequence "http://localhost:8080"))

;; On adding documents to the first server, they should appear on the second
;; They should have the same etags as the first

#_ (c/put-document "http://localhost:8080" "new-doc" { :hello "world"})
#_ (c/get-document "http://localhost:8080" "new-doc")
#_ (c/get-document "http://localhost:8081" "new-doc")

;; On updating documents in the first server, they should update in the second
;; They should have updated etags the same as the first

#_ (c/put-document "http://localhost:8080" "great-doc" { :hello "world"})
#_ (c/put-document "http://localhost:8080" "great-doc" { :hello "bob"})
#_ (c/get-document "http://localhost:8081" "great-doc")

;; If I shut down the slave, it should continue from where it left off

;; If I shut down the master, the slave should gracefully await instruction


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




