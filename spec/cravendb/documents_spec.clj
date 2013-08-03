(ns cravendb.documents-spec
  (:use [speclj.core])
  (:require [me.raynes.fs :as fs] 
            [cravendb.documents :as docs]))

(defn clear-test-data []
  (fs/delete-dir "testdir"))

(defn open-test-db []
  (docs/db "testdir"))

(describe "Various db operations"
  (before (clear-test-data))
  (after (clear-test-data))
  (it "can put and get a document"
    (let [db (open-test-db)]
      (.-put db "1" "hello")
      (should (= (.-get db "1") "hello"))
      (.close db)))
  (it "returns nil for a non-existent document"
    (let [db (open-test-db)]
      (should (= (.-get db "1337") nil))
      (.close db)))
  (it "can delete a document"
    (let [db (open-test-db)]
      (.-put db "1" "hello")
      (.-delete db "1")
      (should (= (.-get db "1") nil))
      (.close db))))
