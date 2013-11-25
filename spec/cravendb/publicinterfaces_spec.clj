(ns cravendb.publicinterfaces-spec
  (require [cravendb.client :as client]
           [serializable.fn :as s]
           [speclj.core :refer :all]
           [cravendb.database :as db]
           [me.raynes.fs :as fs]
           [cravendb.embedded :as embedded]
           [cravendb.remote :as remote]
           [cravendb.testing :refer :all]))

(defmacro with-remote [& body]
  `(with-test-server 
     (fn [] ~@body)))

(defmacro multi [description & body]
  `(describe "with the various storage mediums"
    (describe "embedded in-memory"
      (it ~description
        (with-open [~'instance (embedded/create)] ~@body)))
   (describe "embedded on-disk"
    (it ~description
      (with-open [~'instance (embedded/create :path "testdir")] ~@body)
      (fs/delete-dir "testdir")))                
    (describe "remote"
      (it ~description
        (with-remote
          (with-open [~'instance (remote/create :href "http://localhost:9000")] ~@body))))))

(describe "Basic public API usage"
  (describe "Non existent documnts"
    (multi "will return nil"
      (should-be-nil (db/load-document instance "1337"))))

  (describe "Round tripping the document"
    (multi "will be able to retrieve the document"
      (db/put-document instance "1" { :greeting "hello world"} {})
      (should== 
        {:greeting "hello world"} 
        (db/load-document instance "1"))))

  (describe "Deleting a document"
    (multi "will return nil for a deleted document"
      (db/put-document instance "1" "hello world" {})
      (db/delete-document instance "1" {})
      (should-be-nil (db/load-document instance "1"))))
          
  (describe "round-tripping a clojure map"
    (multi "will return the exact map"                                
      (db/put-document instance "1" { :id 1 :text "hello world" } {})
      (should== { :id 1 :text "hello world"}
        (db/load-document instance "1"))))

  (describe "round-tripping a sequence"
    (multi "will return the exact sequence"                                
      (db/put-document instance "1" '(1 2 3 4) {})
      (should== '(1 2 3 4)
         (db/load-document instance "1"))))

  (describe "round-tripping a vector"
    (multi "will return the exact vector"                                
      (db/put-document instance "1" [1 2 3 4] {})
      (should== [1 2 3 4]
        (db/load-document instance "1"))))
       
 (describe "round-tripping an index"
  (multi "will be retrievable by id"
    (db/put-index instance
      { :id "by_username"
        :map "(fn [doc] {\"username\" (:username doc)})" })
    (should= "(fn [doc] {\"username\" (:username doc)})"
      (:map (db/load-index instance "by_username")))))       

  (describe "querying a custom index", 
    (multi "will return documents matching the query"
      (db/put-index instance
        { :id  "by_username" 
          :map "(fn [doc] {\"username\" (:username doc)})"})
      (db/put-document instance "1" { :username "bob"} {}) 
      (db/put-document instance "2" { :username "alice"} {})
      (should== '({:username "bob"}) 
        (db/query instance
          { :filter "(= \"username\" \"bob\")" :index "by_username" :wait true})))) 

  (describe "querying for a deleted document"
    (multi "will return no results"
      (db/put-index instance
       { :id "by_username" 
         :map "(fn [doc] {\"username\" (:username doc)})"})
      (db/put-document instance "1" { :username "bob"} {})
      (db/query instance { :index "by_username" :wait true })    
      (db/delete-document instance "1" {})
      (should
        (empty? (db/query instance { :filter "(= \"username\" \"bob\")" 
                                     :index "by_username" 
                                     :wait true}))))) 


          )

  









