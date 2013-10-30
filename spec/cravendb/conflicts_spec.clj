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

(describe "writing two documents in succession with no history specified"
  (it "will generate no conflicts"
  (with-full-setup (fn [{:keys [storage] :as instance}]
    (db/put-document instance "1" "hello world")                  
    (db/put-document instance "1" "hello world")                  
    (should= 0 (count (docs/conflicts storage)))))))

(describe "writing two documents, with valid history specified"
  (it "will generate no conflicts"
  (with-full-setup (fn [{:keys [storage] :as instance}]
    (db/put-document instance "1" "hello world")                  
    (db/put-document instance "1" "hello world" (docs/load-document-metadata storage "1"))
    (should= 0 (count (docs/conflicts storage)))))))

(describe "writing a document with an old history"
  (with-all 
    results
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (db/put-document instance "1" "hello world")
        (let [old-meta (docs/load-document-metadata storage "1")]
        (db/put-document instance "1" "hello world")   
        (db/put-document instance "1" "hello bob" old-meta)   
        {
         :conflicts (docs/conflicts storage)
         :document (docs/load-document storage "1")
         }))))
  (it "will generate a conflict"
    (should= 1 (count (:conflicts @results))))
  (it "will have the document id in the conflict information"
    (should= "1" (:id (first (:conflicts @results)))))
  (it "will have a new synctag in the conflict information"
    (should= (integer-to-synctag 3) (get-in (first (:conflicts @results)) [:metadata :synctag])))
  (it "will have the alternative document in the conflict information"
    (should= "hello bob" (:data (first (:conflicts @results)))))
  (it "will leave the original document intact"
    (should= "hello world" (:document @results))))

(describe "Clearing the conflicts for a document"
  (with-all result
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (db/put-document instance "1" "hello world")
      (db/put-document instance "2" "hello world")
      (let [old-meta-one (docs/load-document-metadata storage "1") 
            old-meta-two (docs/load-document-metadata storage "2")]
        (db/put-document instance "1" "hello world")   
        (db/put-document instance "2" "hello world")   
        (db/put-document instance "1" "hello bob" old-meta-one)   
        (db/put-document instance "2" "hello bob" old-meta-two)   
        (db/clear-conflicts instance "1")
        {
         :conflicts (docs/conflicts storage)})))) 
  (it "will remove all the conflicts for that document"
    (should= 1 (count (:conflicts @result))))
  (it "will leave the conflicts for other documents"
    (should== ["2"] (map :id (:conflicts @result)))))
