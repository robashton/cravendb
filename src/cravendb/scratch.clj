(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:require [cravendb.database :as db]
            [cravendb.transaction :as t]
            [cravendb.testing :refer :all]
            [cravendb.client :as client]))

#_ (with-test-server 
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
   #spy/p      (client/query 
          "http://localhost:9000" 
          { :filter "(= \"username\" \"bob\")" :index "by_username" :wait true}))) 



#_ (do (flatten (seq { :filter "blah" :foo "cake"})))
