(ns cravendb.documents-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.indexes]))

(describe "Various db operations"
  (it "can put and get a document"
    (with-db (fn [db]
      (.store db "1" "hello")
      (should (= (.load db "1") "hello")))))
  (it "returns nil for a non-existent document"
    (with-db (fn [db]
      (should (= (.load db "1337") nil))
      (.close db))))
  (it "can delete a document"
    (with-db (fn [db]
      (.store db "1" "hello")
      (.delete db "1")
      (should (= (.load db "1") nil))
      (.close db)))))

(describe "Etags"
  (it "will have an etag starting at zero before anything is written"
    (with-db (fn [db]
      (should= (.last-etag db) 0))))
  (it "Will have an etag greater than zero after committing a single document"
    (with-db (fn [db]
      (.store db "1" "hello")
      (should (> (.last-etag db) 0)))))
  (it "links an etag with a document upon writing"
    (with-db (fn [db]
      (.store db "1" "hello")
      (should (> (.get-etag db "1") 0)))))
  (it "can retrieve documents written since an etag"
    (with-db (fn [db]
      (.store db "1" "hello")
      (let [etag (.last-etag db)]
        (.store db "2" "hello")
        (.store db "3" "hello")
        (.written-since-etag db etag 
          (fn [items]
            (should== '("2" "3") items))))))))

