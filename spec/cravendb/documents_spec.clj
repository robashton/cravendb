(ns cravendb.documents-spec
  (:require [cravendb.documents :as docs])
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.indexes]))

(describe "Various db operations"
  (it "can put and get a document"
    (with-db (fn [db]
      (docs/store db "1" "hello")
      (should (= (docs/load db "1") "hello")))))
  (it "returns nil for a non-existent document"
    (with-db (fn [db]
      (should (= (docs/load db "1337") nil)))))
  (it "can delete a document"
    (with-db (fn [db]
      (docs/store db "1" "hello")
      (docs/delete db "1")
      (should (= (docs/load db "1") nil))))))

(describe "Etags"
  (it "will have an etag starting at zero before anything is written"
    (with-db (fn [db]
      (should= (docs/last-etag db) 0))))
  (it "Will have an etag greater than zero after committing a single document"
    (with-db (fn [db]
      (docs/store db "1" "hello")
      (should (> (docs/last-etag db) 0)))))
  (it "links an etag with a document upon writing"
    (with-db (fn [db]
      (docs/store db "1" "hello")
      (should (> (docs/get-etag db "1") 0)))))
  (it "can retrieve documents written since an etag"
    (with-db (fn [db]
      (docs/store db "1" "hello")
      (let [etag (docs/last-etag db)]
        (docs/store db "2" "hello")
        (docs/store db "3" "hello")
        (docs/written-since-etag db etag 
          (fn [items]
            (should== '("2" "3") items))))))))

