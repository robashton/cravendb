(ns cravendb.storage-spec
  (:use [speclj.core]
        [cravendb.testing])
  (:require [cravendb.storage :as s]
            [me.raynes.fs :as fs]))
   

(describe "Putting and retrieving an object during a transaction"
  (it "will be able to retrieve the object"
    (with-db (fn [db]
      (with-open [tx (s/ensure-transaction db)]
        (should= "hello"
          (-> tx
            (s/store "1" "hello")
            (s/get-string "1"))))))))

(describe "Storing, deleting, and getting an object during a transaction"
  (it "will be able to retrieve the object"
    (with-db (fn [db]
      (with-open [tx (s/ensure-transaction db)]
        (should= nil
          (-> tx
            (s/store "1" "hello")
            (s/delete "1")
            (s/get-string "1"))))))))

(describe "Storing, deleting, storing and getting an object during a transaction"
  (it "will be able to retrieve the object"
    (with-db (fn [db]
      (with-open [tx (s/ensure-transaction db)]
        (should= "hello again"
          (-> tx
            (s/store "1" "hello")
            (s/delete "1")
            (s/store "1" "hello again")
            (s/get-string "1"))))))))

(describe "Asking for an object that doesn't exist in the transaction, but exists in the backing store"
  (it "will receive the object from the backing store"
      (with-db 
        (fn [db]
          (with-open [tx (s/ensure-transaction db)]
             (-> tx
               (s/store "1" "hello")
               (s/commit!)))
          (with-open [tx (s/ensure-transaction db)]
             (should= "hello"
              (-> tx
                (s/get-string "1"))))))))

(describe "Asking for an object written during this transaction"
  (it "will not receive the object"
      (with-db 
        (fn [db]
          (with-open [tx (s/ensure-transaction db)
                      tx2 (s/ensure-transaction db)]
             (-> tx
               (s/store "1" "hello")
               (s/commit!))
            (should= nil (s/get-string tx2 "1")))))))

;; This can happen on shut-down sometimes
;;(describe "closing a tx after storage has been closed"
;;  (it "will not fall over screaming"
;;    (let [tx (atom {})] 
;;      (with-db
;;        (fn [db]
;;          (swap! tx #(do (println %1) (s/ensure-transaction db)))))
;;      (.close @tx)))) 

(describe "Deleting an object that does not exist"
  (it "will achieve nothing"
      (with-db 
        (fn [db]
          (with-open [tx (s/ensure-transaction db)
                      tx2 (s/ensure-transaction db)]
             (-> tx
               (s/delete "1")
               (s/commit!))
            (should= nil (s/get-string tx2 "1")))))))
