(ns cravendb.masterslavereplication-spec
  (:require [cravendb.database :as db]
            [cravendb.client :as c]
            [cravendb.testing :refer [start-server stop-server]]
            )
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
  (it "will stream 1024 documents at a time"
    (should= 1024 (count (c/stream-seq (:url @master)))))
  (it "will start a page from the next etag specified"
    (let [stream (c/stream-seq (:url @master))
          first-etag (get-in (first stream) [:metadata :etag])
          second-etag (get-in (second stream) [:metadata :etag])]
      (should= second-etag
        (get-in (first (c/stream-seq (:url @master) first-etag)) [:metadata :etag])) ))
          


)
