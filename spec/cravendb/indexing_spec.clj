(ns cravendb.indexing-spec
  (:use [speclj.core]
        [cravendb.testing])
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.storage :as storage]))

(def test-index {:name "test" :map (fn [doc] (doc :author))})

(def write-three-documents 
  (fn [db]
    (with-open [tx (.ensure-transaction db)]
      (-> tx
        (docs/store-document "1" (pr-str { :title "hello" :author "rob"}))
        (docs/store-document "2" (pr-str { :title "morning" :author "vicky"}))
        (docs/store-document "3" (pr-str { :title "goodbye" :author "james"}))
        (.commit!)))))

(def write-one-document 
  (fn [db]
    (with-open [tx (.ensure-transaction db)]
      (-> tx
        (docs/store-document "2" (pr-str { :title "morning" :author "vicky"}))
        (.commit!)))))

(describe "indexing some documents"
  (it "will index all the documents"
    (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db [test-index])
        (with-open [tx (.ensure-transaction db)]
          (should= 3 (indexing/last-index-doc-count tx))
          (should= (docs/last-etag tx) (indexing/last-indexed-etag tx)))))))

(describe "indexing new documents"
  (it "will index only the new documents"
    (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db [test-index])
        (write-one-document db)
        (indexing/index-documents! db [test-index])
        (with-open [tx (.ensure-transaction db)]
          (should= (docs/last-etag tx) (indexing/last-indexed-etag tx))
          (should= 1 (indexing/last-index-doc-count tx)))))))
