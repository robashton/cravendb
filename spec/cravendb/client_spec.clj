(ns cravendb.client-spec
  (:use [speclj.core]
        [cravendb.testing])
  (:require [cravendb.client :as client]))

(describe "Basic client operations"
  (it "returns null for a non-existent document"
    (with-test-server (fn [] 
      (-> 
        (client/load-document "http://localhost:9000" "1337")
        (should-be-nil)))))
  (it "should be able to PUT and GET a document"
    (with-test-server (fn [] 
      (client/put-document "http://localhost:9000" "1" "hello world")
      (-> (client/load-document "http://localhost:9000" "1")
          (should= "hello world")))))
  (it "be able to DELETE a document"
    (with-test-server (fn [] 
      (client/put-document "http://localhost:9000" "1" "hello world")
      (client/delete-document "http://localhost:9000" "1")
      (-> 
        (client/load-document "http://localhost:9000" "1")
        (should-be-nil))))))

(describe "passing clojure structures as documents"
  (it "should be able to put a map and retrieve it"
    (with-test-server (fn []
      (client/put-document "http://localhost:9000" "1" { :id 1 :text "hello world" })
      (-> (client/load-document "http://localhost:9000" "1")
          (should== { :id 1 :text "hello world"})))))

  (it "should be able to put a sequence and retrieve it"
    (with-test-server (fn []
      (client/put-document "http://localhost:9000" "1" '(1 2 3 4))
      (-> (client/load-document "http://localhost:9000" "1")
          (should== '(1 2 3 4))))))

  (it "should be able to put a vector and retrieve it"
    (with-test-server (fn []
      (client/put-document "http://localhost:9000" "1" [1 2 3 4])
      (-> (client/load-document "http://localhost:9000" "1")
          (should== [1 2 3 4]))))))
