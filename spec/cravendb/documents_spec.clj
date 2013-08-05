(ns cravendb.documents-spec
  (:use [speclj.core])
  (:require [me.raynes.fs :as fs] 
            [cravendb.documents :as docs]))

(defn clear-test-data []
  (fs/delete-dir "testdir"))

(defn open-test-db []
  (docs/db "testdir"))

(defn with-db [testfn]
  (clear-test-data)
  (let [db (open-test-db)]
    (try 
      (testfn db)
      (finally
        (.close db)
        (clear-test-data)))))

(describe "Various db operations"
  (it "can put and get a document"
    (with-db (fn [db]
      (.-put db "1" "hello")
      (should (= (.-get db "1") "hello")))))
  (it "returns nil for a non-existent document"
    (with-db (fn [db]
      (println "zomg")
      (should (= (.-get db "1337") nil))
      (.close db))))
  (it "can delete a document"
    (with-db (fn [db]
      (.-put db "1" "hello")
      (.-delete db "1")
      (should (= (.-get db "1") nil))
      (.close db)))))
