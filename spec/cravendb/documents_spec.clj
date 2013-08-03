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
  (it "put and retrieve a document"
    (let [db (open-test-db)]
      (.-put db "1" "hello")
      (should (= (.-get db "1") "hello"))
      (.close db))))
