(ns cravendb.defaultindex-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core]
        )
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexengine :as indexengine]
            [cravendb.indexstore :as indexes]
            [cravendb.query :as query]
            [cravendb.lucene :as lucene]
            [cravendb.storage :as s]))

(def write-three-documents 
  (fn [db]
    (with-open [tx (s/ensure-transaction db)]
      (-> tx
        (docs/store-document "doc-1" (pr-str { :title "hello" :author "rob"}))
        (docs/store-document "doc-2" (pr-str { :title "morning" :author "vicky"}))
        (docs/store-document "doc-3" (pr-str { :title "goodbye" :author "james"}))
        (s/commit!)))))

(defn create-invalid-index []  
  (let [storage (lucene/create-memory-index)]
                    {
                    :id "invalid" 
                    :map (fn [doc] {"hello" ((:blah doc) :foo)})
                    :storage storage
                    :writer (lucene/open-writer storage) }))

(defn create-valid-index []  
  (let [storage (lucene/create-memory-index)]
                    {
                    :id "valid" 
                     :map (fn [doc] {"hello" (:author doc)})
                    :storage storage
                    :writer (lucene/open-writer storage) }))

(defn create-test-indexes [] [ (create-invalid-index) (create-valid-index) ])

(describe "Marking an index as failed"
  (with indexes [ (create-invalid-index) (create-valid-index)])

  (it "will mark an index as failed if it throws exceptions"
    (with-db (fn [db]
      (write-three-documents db)
      (indexing/index-documents! db [(create-invalid-index)])
      (should (indexes/is-failed db "invalid")))))

  (it "will mark the documents as indexed regardless of failure"
    (with-db (fn [db]
      (write-three-documents db)
      (let [last-etag (docs/last-etag-in db)]
        (indexing/index-documents! db [(create-invalid-index)])   
        (should= last-etag (indexing/last-indexed-etag db)))))) 

  (it "will not use this index in further indexing processes"
    (with-db (fn [db]
      (write-three-documents db)
      (let [last-etag (docs/last-etag-in db)
            index (create-invalid-index) ]
        (indexing/index-documents! db [index])       
        (write-three-documents db)
        (indexing/index-documents! db [index])       
        (should= last-etag (indexing/last-indexed-etag db)))))) 

  (it "will carry on indexing non-broken indexes"
    (with-db (fn [db]
      (write-three-documents db)
      (indexing/index-documents! db @indexes) 
      (write-three-documents db)
      (let [last-etag (docs/last-etag-in db)]
        (indexing/index-documents! db @indexes)   
        (should= last-etag (indexing/last-indexed-etag db)))))))


(describe "Resetting an index"
  (with indexes [ (create-invalid-index) (create-valid-index)])

  (it "will reset the last indexed etag for that index"
    (with-db (fn [db]
      (write-three-documents db)
      (indexing/index-documents! db @indexes)
      (with-open [tx (s/ensure-transaction db)]
        (s/commit! (indexes/reset-index tx "invalid")))
      (should= (zero-etag) (indexes/get-last-indexed-etag-for-index db "invalid")))) )
  (it "will mark the index as not failed"
     (with-db (fn [db]
      (write-three-documents db)
      (indexing/index-documents! db @indexes)
      (with-open [tx (s/ensure-transaction db)]
        (s/commit! (indexes/reset-index tx "invalid")))
      (should-not (indexes/is-failed db "invalid")))))

          )
