(ns cravendb.storage-spec
  (:use [speclj.core])
  (:require [cravendb.storage :as storage]
            [me.raynes.fs :as fs]))
   
(defn clear-test-data []
  (fs/delete-dir "testdir"))

(defn with-db [testfn]
  (clear-test-data)
  (with-open [db (storage/create-storage "testdir")]
    (testfn db)))

(describe "Putting and retrieving an object during a transaction"
  (it "will be able to retrieve the object"
    (with-db (fn [db]
      (with-open [tx (.ensure-transaction db)]
        (should= "hello"
          (-> tx
            (.store-blob "1" "hello")
            (.get-blob "1"))))))))

(describe "Storing, deleting, and getting an object during a transaction"
  (it "will be able to retrieve the object"
    (with-db (fn [db]
      (with-open [tx (.ensure-transaction db)]
        (should= nil
          (-> tx
            (.store-blob "1" "hello")
            (.delete-blob "1")
            (.get-blob "1"))))))))

(describe "Storing, deleting, storing and getting an object during a transaction"
  (it "will be able to retrieve the object"
    (with-db (fn [db]
      (with-open [tx (.ensure-transaction db)]
        (should= "hello again"
          (-> tx
            (.store-blob "1" "hello")
            (.delete-blob "1")
            (.store-blob "1" "hello again")
            (.get-blob "1"))))))))

(describe "Asking for an object that doesn't exist in the transaction, but exists in the backing store"
  (it "will receive the object from the backing store"
      (with-db 
        (fn [db]
          (with-open [tx (.ensure-transaction db)]
             (-> tx
               (.store-blob "1" "hello")
               (.commit)))
          (with-open [tx (.ensure-transaction db)]
             (should= "hello"
              (-> tx
                (.get-blob "1"))))))))

(describe "Asking for an object written during this transaction"
  (it "will not receive the object"
      (with-db 
        (fn [db]
          (with-open [tx (.ensure-transaction db)
                      tx2 (.ensure-transaction db)]
             (-> tx
               (.store-blob "1" "hello")
               (.commit))
            (should= nil (.get-blob tx2 "1")))))))
