(ns cravendb.masterslavereplication-spec
  (:require [cravendb.database :as db]
            [cravendb.client :as c]
            [cravendb.testing :refer [start-server stop-server]]
            [cravendb.replication :as replication]
            [cravendb.documents :as docs])

  (:use [speclj.core]))

(defn insert-5000-documents [instance] 
  (db/bulk instance
      (map (fn [i]
      {
        :operation :docs-put
        :id (str "docs-" i)
        :document { :whatever (str "Trolololol" i)} 
        }) (range 0 5000)))) 

(describe "Getting a stream of documents from a source server"
  (with-all master (start-server))
  (before-all 
    (insert-5000-documents (:instance @master)))
  (after-all (stop-server @master))

  (it "will stream all of the documents"
    (should= 5000 (count (c/stream-seq (:url @master)))))

  (it "will start a page from the next etag specified"
    (let [stream (c/stream-seq (:url @master))
          first-etag (get-in (first stream) [:metadata :etag])
          second-etag (get-in (second stream) [:metadata :etag])]
      (should= second-etag
        (get-in (first (c/stream-seq (:url @master) first-etag)) [:metadata :etag])) )))

(describe "Bringing up a slave when a master already has documents"
  (with-all master (start-server 8080))
  (with-all slave (start-server 8081))
  (with-all replicator (replication/create (:instance @slave) (:url @master)))

  (before-all 
    (insert-5000-documents (:instance @master))
    (replication/start @replicator)
    (replication/wait-for @replicator 
                          (docs/last-etag-in (get-in @master [:instance :storage]))))

  (after-all
    (stop-server @master)
    (stop-server @slave)
    (replication/stop @replicator))

  (it "will contain all of the documents from the master"
    (should= 5000 (count (c/stream-seq (:url @slave)))))

  (it "will contain identical metadata"
     (should==
       (map :metadata (c/stream-seq (:url @master))) 
       (map :metadata (c/stream-seq (:url @slave)))))

  (it "will contain identical documents"
     (should==
       (map :metadata (c/stream-seq (:url @master))) 
       (map :metadata (c/stream-seq (:url @slave))))))

