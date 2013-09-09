(ns cravendb.documents-spec
  (:require [cravendb.documents :as docs])
  (:require [cravendb.storage :as storage])
  (:use [speclj.core]
        [cravendb.testing]))

(describe "Various db operations"
  (it "can put and get a document"
    (inside-tx (fn [tx]
      (should= "hello"
        (-> tx
          (docs/store-document "1" "hello")
          (docs/load-document "1"))))))
  (it "returns nil for a non-existent document"
    (inside-tx (fn [db]
      (should=  nil (docs/load-document db "1337")))))
  (it "can delete a document"
    (inside-tx (fn [tx]
      (should= nil
        (-> tx
        (docs/store-document "1" "hello")
        (docs/delete-document "1")
        (docs/load-document "1")))))))

(describe "Transactions"
  (it "will load a document from a committed transaction"
      (with-db 
        (fn [db]
          (with-open [tx (.ensure-transaction db)]
            (.commit! (docs/store-document tx "1" "hello")))
          (with-open [tx (.ensure-transaction db)]
            (should= "hello" (docs/load-document tx "1")))))))

(describe "Etags"
  (it "will have an etag starting at zero before anything is written"
    (inside-tx (fn [db]
      (should= (docs/etag-to-integer (docs/last-etag db)) 0))))
  (it "Will have an etag greater than zero after committing a single document"
    (inside-tx (fn [tx]
      (should (>
        (-> tx
          (docs/store-document "1" "hello")
          (docs/last-etag)
          (docs/etag-to-integer)
          )
        0)))))
  (it "links an etag with a document upon writing"
    (inside-tx (fn [tx]
      (should (> 
        (-> tx
          (docs/store-document "1" "hello")
          (docs/etag-for-doc "1")
          (docs/etag-to-integer)) 
        0)))))

  (it "can retrieve documents written since an etag"
    (with-db (fn [db]
      (with-open [tx (.ensure-transaction db)] 
        (let [tx (docs/store-document tx "1" "hello")
             etag (docs/last-etag tx) ]
          (-> tx
            (docs/store-document "2" "hello")
            (docs/store-document "3" "hello")
            (.commit!))
          (with-open [tx (.ensure-transaction db)]
            (with-open [iter (.get-iterator tx)]
              (should== '("2" "3") (docs/iterate-etags-after iter etag))))))))))
