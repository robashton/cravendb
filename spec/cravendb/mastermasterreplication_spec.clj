(ns cravendb.mastermasterreplication-spec
  (:require [cravendb.database :as db]
            [cravendb.client :as c]
            [cravendb.storage :as s]
            [cravendb.testing :refer [start-server stop-server]]
            [cravendb.replication :as r]
            [cravendb.documents :as docs])

  (:use [speclj.core]))

(describe 
  "Various scenarios between servers 'one' and 'two"
    (with one (start-server 8080 :server-id "one"))
    (with two (start-server 8081 :server-id "two"))
    (with one->two (fn [] (r/pump-replication (get-in @two [:instance :storage]) (:url @one))))
    (with two->one (fn [] (r/pump-replication (get-in @one [:instance :storage]) (:url @two))))
    (after
      (stop-server @one)
      (stop-server @two))

  (describe 
    "Writing a new document to 'one'"

    (before
      (c/put-document (:url @one) "doc-1" { :hello "world"})
      (@one->two))

    (it "will cause the 'two' contain the new document"
      (should== { :hello "world"} (c/get-document (:url @two) "doc-1"))))
  
  (describe
    "Writing new documents to both 'one' and 'two"
    (before
      (c/put-document (:url @one) "doc-1" { :hello "1"})
      (c/put-document (:url @two) "doc-2" { :hello "2"})
      (@one->two)
      (@two->one))

    (it "will cause both documents to be on 'one'"
       (should== { :hello "1"} (c/get-document (:url @one) "doc-1"))  
       (should== { :hello "2"} (c/get-document (:url @one) "doc-2")))

    (it "will cause both documents to be on 'two'"
       (should== { :hello "1"} (c/get-document (:url @two) "doc-1"))  
       (should== { :hello "2"} (c/get-document (:url @two) "doc-2"))))


    ;; NOTE: I don't actually know what my desired behaviour is for this yet
    ;; But I'm rolling with this as it means no data loss
    (describe
        "Writing conflicting documents to both 'one' and 'two'"
        (before
          (c/put-document (:url @one) "doc-1" { :hello "1"})
          (c/put-document (:url @two) "doc-1" { :hello "2"})
          (@one->two)
          (@two->one))

        (it "will have the original document kept on 'one"
          (should== { :hello "1"} (c/get-document (:url @one) "doc-1")))

        (it "will have the original document kept on 'two"
          (should== { :hello "2"} (c/get-document (:url @two) "doc-1")))

        (it "will have a conflict document on 'one'"
          (should= 1 (count (c/get-conflicts (:url @one)))))

        (it "will have a conflict document on 'two'"
          (should= 1 (count (c/get-conflicts (:url @two))))))

    (describe
      "Updating a document on 'two' that was created on 'one"
        (before
          (c/put-document (:url @one) "doc-1" { :hello "1"})
          (@one->two)
          (c/put-document (:url @two) "doc-1" { :hello "2"})
          (@two->one))

      (it "will have the new document on 'one'"
        (should== { :hello "2"} (c/get-document (:url @one) "doc-1")))
      (it "will have the new document on 'two'"
         (should== { :hello "2"} (c/get-document (:url @two) "doc-1")))
      (it "will have no conflicts on 'one'"
          (should= 0 (count (c/get-conflicts (:url @one)))))
      (it "will have no conflicts on 'two'"
          (should= 0 (count (c/get-conflicts (:url @two)))))))
