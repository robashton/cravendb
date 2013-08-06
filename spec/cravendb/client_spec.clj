(ns cravendb.client-spec
  (:use [speclj.core]
        [cravendb.testing])
  (:require [cravendb.client :as client]))

(describe "Various client operations"
  (it "returns null for a non-existent document"
    (with-test-server (fn [] 
      (should (= (client/load-document "http://localhost:9000" "1337") nil)))))
  (it "should be able to PUT and GET a document"
    (with-test-server (fn [] 
      (client/put-document "http://localhost:9000" "1" "hello world")
      (should (= (client/load-document "http://localhost:9000" "1") "hello world"))))))

