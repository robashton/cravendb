(ns cravendb.client-spec
  (:use [speclj.core]
        [cravendb.testing])
  (require [cravendb.client :as client]
           [serializable.fn :as s]))

(describe "Basic client operations"
  (it "returns null for a non-existent document"
    (with-test-server (fn [] 
      (-> 
        (client/get-document "http://localhost:9000" "1337")
        (should-be-nil)))))
  (it "should be able to PUT and GET a document"
    (with-test-server (fn [] 
      (client/put-document "http://localhost:9000" "1" { :greeting "hello world"})
      (->> (client/get-document "http://localhost:9000" "1")
          (should== {:greeting "hello world"})))))

  (it "be able to DELETE a document"
    (with-test-server (fn [] 
      (client/put-document "http://localhost:9000" "1" "hello world")
      (client/delete-document "http://localhost:9000" "1")
      (-> 
        (client/get-document "http://localhost:9000" "1")
        (should-be-nil))))))

(describe "passing clojure structures as documents"
  (it "should be able to put a map and retrieve it"
    (with-test-server (fn []
      (client/put-document "http://localhost:9000" "1" { :id 1 :text "hello world" })
      (-> (client/get-document "http://localhost:9000" "1")
          (should== { :id 1 :text "hello world"})))))

  (it "should be able to put a sequence and retrieve it"
    (with-test-server (fn []
      (client/put-document "http://localhost:9000" "1" '(1 2 3 4))
      (-> (client/get-document "http://localhost:9000" "1")
          (should== '(1 2 3 4))))))

  (it "should be able to put a vector and retrieve it"
    (with-test-server (fn []
      (client/put-document "http://localhost:9000" "1" [1 2 3 4])
      (-> (client/get-document "http://localhost:9000" "1")
          (should== [1 2 3 4]))))))

(describe "Creating an index on the server", 
  (it "will be retrievable once on the server"
    (with-test-server 
      (fn []
        (client/put-index 
          "http://localhost:9000" 
          "by_username" 
          "(fn [doc] {\"username\" (:username doc)})")
        (should=
          "(fn [doc] {\"username\" (:username doc)})"
          ((client/get-index "http://localhost:9000" "by_username") :map))))))

(describe "Querying an index on the server", 
  (it "will return documents matching the query"
    (with-test-server 
      (fn []
        (client/put-index 
          "http://localhost:9000" 
          "by_username" 
          "(fn [doc] {\"username\" (:username doc)})")
        (client/put-document 
          "http://localhost:9000" 
          "1" { :username "bob"})
        (client/put-document 
          "http://localhost:9000" 
          "2" { :username "alice"})
        (should== 
          '({:username "bob"}) 
          (client/query 
            "http://localhost:9000" 
            { :query "(= \"username\" \"bob\")" :index "by_username" :wait true}))))))


(describe "Querying for a deleted document"
  (it "will return no results"
     (with-test-server 
      (fn []
        (client/put-index 
          "http://localhost:9000" 
          "by_username" 
          "(fn [doc] {\"username\" (:username doc)})")
        (client/put-document 
          "http://localhost:9000" 
          "1" { :username "bob"})
        (client/query 
          "http://localhost:9000" 
          { :query "(= \"username\" \"bob\")" :index "by_username" :wait true})    
        (client/delete-document "http://localhost:9000" "1" )
        (should= 0 
          (count 
            (client/query 
              "http://localhost:9000" 
              { :query "(= \"username\" \"bob\")" :index "by_username" :wait true})))))))
