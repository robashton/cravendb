(ns cravendb.conflicts-spec
  (:require [cravendb.documents :as docs]
            [cravendb.database :as db]
            [speclj.core :refer :all]    
            [cravendb.testing :refer :all]
            [cravendb.core :refer :all]))

(describe "writing a single document"
  (multi "will generate no conflicts"
    (db/put-document instance "1" "hello world" {})                  
    (should= 0 (count (db/conflicts instance)))))

(describe "writing two documents in succession with no history specified"
  (multi "will generate no conflicts"
    (db/put-document instance "1" "hello world" {})                  
    (db/put-document instance "1" "hello world" {})                  
    (should= 0 (count (db/conflicts instance)))))

(describe "writing two documents, with valid history specified"
  (multi "will generate no conflicts"
    (db/put-document instance "1" "hello world" {})                  
    (db/put-document instance "1" "hello world" (db/load-document-metadata instance "1"))
    (should= 0 (count (db/conflicts instance)))))

(defn generate-conflict [instance]
  (db/put-document instance "1" "hello world" {})
  (let [old-meta (db/load-document-metadata instance "1")]
    (db/put-document instance "1" "hello world"  {})   
    (db/put-document instance "1" "hello bob" old-meta)   
    {
     :conflicts (db/conflicts instance)
     :document (db/load-document instance "1") }))

(describe "writing a document with an old history"
  (multi "will generate a conflict"
    (should= 1 (count (:conflicts (generate-conflict instance)))))
  (multi "will have the document id in the conflict information"
    (should= "1" (:id (first (:conflicts (generate-conflict instance))))))
  (multi "will have a new synctag in the conflict information"
    (should= (integer-to-synctag 3) (get-in (first (:conflicts (generate-conflict instance))) [:metadata :synctag])))
  (multi "will have the alternative document in the conflict information"
    (should= "hello bob" (:data (first (:conflicts (generate-conflict instance))))))
  (multi "will leave the original document intact"
    (should= "hello world" (:document (generate-conflict instance)))))

(defn generate-and-clear [instance]
  (db/put-document instance "1" "hello world" {})
  (db/put-document instance "2" "hello world" {})
  (let [old-meta-one (db/load-document-metadata instance "1") 
        old-meta-two (db/load-document-metadata instance "2")]
    (db/put-document instance "1" "hello world" {})   
    (db/put-document instance "2" "hello world" {})   
    (db/put-document instance "1" "hello bob" old-meta-one)   
    (db/put-document instance "2" "hello bob" old-meta-two)   
    (db/clear-conflicts instance "1")
    {
     :conflicts (db/conflicts instance)}))

(describe "Clearing the conflicts for a document"
  (multi "will remove all the conflicts for that document"
    (should= 1 (count (:conflicts (generate-and-clear instance)))))
  (multi "will leave the conflicts for other documents"
    (should== ["2"] (map :id (:conflicts (generate-and-clear instance))))))
