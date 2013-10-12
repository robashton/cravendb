(ns cravendb.etags-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])

  (:require [cravendb.storage :as s]
            [cravendb.documents :as docs]
            [cravendb.database :as db]))

(describe "Etags"
  (it "will have an etag starting at zero before anything is written"
    (inside-tx (fn [db]
      (should= (etag-to-integer (docs/last-etag-in db)) 0))))

  (it "Will have an etag greater than zero after committing a single document"
    (with-full-setup (fn [instance]
      (db/put-document instance "1" "hello")
      (should 
        (< 0 (etag-to-integer (docs/last-etag-in (:storage instance))))))))

  (it "links an etag with a document upon writing"
    (inside-tx (fn [tx]
      (should= "1337" (-> tx
          (docs/store-document "1" "hello" "1337")
          (docs/etag-for-doc "1"))))))

  (it "can retrieve documents written since an etag"
    (with-db (fn [db]
      (with-open [tx (s/ensure-transaction db)] 
        (->
          (docs/store-document tx "1" "hello" (integer-to-etag 1)) 
          (docs/store-document "2" "hello" (integer-to-etag 2))
          (docs/store-document "3" "hello" (integer-to-etag 3))
          (s/commit!))
        (with-open [tx (s/ensure-transaction db)]
          (with-open [iter (s/get-iterator tx)]
            (should== '("2" "3") (docs/iterate-etags-after iter (integer-to-etag 1)))))))))) 
