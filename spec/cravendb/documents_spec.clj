(ns cravendb.documents-spec
  (:require [cravendb.documents :as docs])
  (:require [cravendb.storage :as s])
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core]))

(describe "Various db operations"
  (it "can put and get a document"
    (inside-tx (fn [tx]
      (should= "hello"
        (-> tx
          (docs/store-document "1" "hello" "001")
          (docs/load-document "1"))))))
  (it "returns nil for a non-existent document"
    (inside-tx (fn [db]
      (should=  nil (docs/load-document db "1337")))))
  (it "can delete a document"
    (inside-tx (fn [tx]
      (should= nil
        (-> tx
        (docs/store-document "1" "hello" "001")
        (docs/delete-document "1")
        (docs/load-document "1")))))))

(describe "Transactions"
  (it "will load a document from a committed transaction"
      (with-db 
        (fn [db]
          (with-open [tx (s/ensure-transaction db)]
            (s/commit! (docs/store-document tx "1" "hello" "001")))
          (with-open [tx (s/ensure-transaction db)]
            (should= "hello" (docs/load-document tx "1")))))))


