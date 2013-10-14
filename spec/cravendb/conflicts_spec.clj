(ns cravendb.conflicts-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])
  (:require [cravendb.documents :as docs]
            [cravendb.database :as db]))


(describe "writing a single document"
  (it "will generate no conflicts"
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (db/put-document instance "1" "hello world")                  
       (should= 0 (count (docs/conflicts storage)))))))

(describe "writing two documents in succession with no etags specified"
  (it "will generate no conflicts"
  (with-full-setup (fn [{:keys [storage] :as instance}]
    (db/put-document instance "1" "hello world")                  
    (db/put-document instance "1" "hello world")                  
    (should= 0 (count (docs/conflicts storage)))))))

(describe "writing two documents, with valid etags specified"
  (it "will generate no conflicts"
  (with-full-setup (fn [{:keys [storage] :as instance}]
    (db/put-document instance "1" "hello world")                  
    (db/put-document instance "1" "hello world" (docs/etag-for-doc storage "1"))
    (should= 0 (count (docs/conflicts storage)))))))

(describe "writing a document with a different etag"
  (with-all 
    results
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (db/put-document instance "1" "hello world")
      (let [old-etag (docs/etag-for-doc storage "1")]
        (db/put-document instance "1" "hello world")   
        (db/put-document instance "1" "hello bob" old-etag)   
        {
         :conflicts (docs/conflicts storage)
         :document (docs/load-document storage "1")
         }))))
  (it "will generate a conflict"
    (should= 1 (count (:conflicts @results))))
  (it "will have the document id in the conflict information"
    (should= "1" (:id (first (:conflicts @results)))))
  (it "will have a new etag in the conflict information"
    (should= (integer-to-etag 3) (:etag (first (:conflicts @results)))))
  (it "will have the alternative document in the conflict information"
    (should= "hello bob" (:data (first (:conflicts @results)))))
  (it "will leave the original document intact"
    (should= "hello world" (:document @results)))
          )
