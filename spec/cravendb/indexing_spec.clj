(ns cravendb.indexing-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])

  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexstore :as indexes]
            [cravendb.lucene :as lucene]
            [cravendb.query :as query]
            [cravendb.indexengine :as indexengine]
            [cravendb.database :as database]
            [cravendb.storage :as s]
            [cravendb.client :as client]
            [cravendb.lucene :as lucene]))

(defn create-test-index []  
  (let [storage (lucene/create-memory-index)]
                    {
                    :id "test" 
                    :map (fn [doc] {"author" (doc :author)})
                    :storage storage
                    :writer (lucene/open-writer storage) }))

(defn create-test-indexes [] [ (create-test-index) ])

(defn parse-results [results]
  results)

(def write-three-documents 
  (fn [db]
    (with-open [tx (s/ensure-transaction db)]
      (-> tx
        (docs/store-document "doc-1" { :title "hello" :author "rob"} (integer-to-synctag 1))
        (docs/store-document "doc-2" { :title "morning" :author "vicky"}(integer-to-synctag 2))
        (docs/store-document "doc-3" { :title "goodbye" :author "james"} (integer-to-synctag 3))
        (s/commit!)))))

(def write-one-document 
  (fn [db]
    (with-open [tx (s/ensure-transaction db)]
      (-> tx
        (docs/store-document "2" { :title "morning" :author "vicky"} (integer-to-synctag 4))
        (s/commit!)))))

(describe "indexing some documents"
  (with test-indexes (create-test-indexes))
  (it "will index all the documents"
    (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db @test-indexes)
        (with-open [tx (s/ensure-transaction db)]
          (should= 3 (indexing/last-index-doc-count tx))
          (should= (integer-to-synctag 3) (indexing/last-indexed-synctag tx)))))))

(describe "indexing new documents"
  (with test-indexes (create-test-indexes))
  (it "will index only the new documents"
    (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db @test-indexes)
        (write-one-document db)
        (indexing/index-documents! db @test-indexes)
        (with-open [tx (s/ensure-transaction db)]
          (should= (integer-to-synctag 4) (indexing/last-indexed-synctag tx))
          (should= 1 (indexing/last-index-doc-count tx)))))))

(describe "loading indexes from the database and querying using them"
  (with test-indexes (create-test-indexes))
  (it "will index all the documents"
    (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db @test-indexes)
        (with-open [reader (lucene/open-reader ((first @test-indexes) :storage))]
            (should== '("doc-2") (lucene/query reader "(= \"author\" \"vicky\")" 10 nil nil)))))))

(describe "querying an index with content in it"
  (with test-indexes (create-test-indexes))
  (it "will return the right document ids"
    (with-db (fn [db]
        (write-three-documents db) 
        (with-open [tx (s/ensure-transaction db)]
          (s/commit! 
            (indexes/put-index tx 
                { :id "by_author" :map "(fn [doc] {\"author\" (doc :author)})"} (integer-to-synctag 6))))

        (with-open [ie (indexengine/create-engine db)]
          (indexing/index-documents! db (indexengine/compiled-indexes ie)))

        (with-open [tx (s/ensure-transaction db)]
          (should= 8 (indexing/last-index-doc-count tx)) ;; The index counts and there is a default index
          (should= (integer-to-synctag 6) (indexing/last-indexed-synctag tx)))))))

(describe "Running indexing with no documents or indexes"
  (it "will not fall over in a heap, crying with a bottle of whisky"
    (with-db (fn [db]
      (with-open [ie (indexengine/create-engine db)]
        (should-not-throw (indexing/index-documents! db (indexengine/compiled-indexes ie))))))))


(describe "Keeping track of per index status"
  (it "will start each tracker off at zero status"
      (with-db (fn [db]
        (with-open [tx (s/ensure-transaction db)]
          (s/commit! (indexes/put-index tx { :id "test" } (integer-to-synctag 1))))
        
        (should= (integer-to-synctag 0) 
                 (indexes/get-last-indexed-synctag-for-index db "test")))))

  (it "will set the tracker to the last indexed synctag"
      (with-db (fn [db]


        (with-open [tx (s/ensure-transaction db)]
          (-> tx
            (docs/store-document "1" { :foo "bar" } (integer-to-synctag 1)) 
            (docs/store-document "2" { :foo "bas" } (integer-to-synctag 2)) 
            (docs/store-document "3" { :foo "baz" } (integer-to-synctag 3))
            (s/commit!)))

        (with-open [tx (s/ensure-transaction db)]
          (s/commit! (indexes/put-index tx 
            { :id "test" :map "(fn [doc] {\"foo\" (:foo doc)})"} (integer-to-synctag 4))))

        (with-open [ie (indexengine/create-engine db)]
          (indexing/index-documents! db (indexengine/compiled-indexes ie)))

        (should= (integer-to-synctag 4) 
                 (indexes/get-last-indexed-synctag-for-index db "test")))))) 

(def by-name-map 
  "(fn [doc] { \"name\" (:name doc) })")

(def by-name-animal-filter
  "(fn [doc metadata] (.startsWith (:id metadata) \"animal-\"))")

(describe "Applying a filter to an index"
  (with-all results (with-full-setup
    (fn [{:keys [storage index-engine] :as instance}]
      (database/put-document instance "animal-1" { :name "zebra"})
      (database/put-document instance "animal-2" { :name "aardvark"})
      (database/put-document instance "animal-3" { :name "giraffe"})
      (database/put-document instance "animal-4" { :name "anteater"})
      (database/put-document instance "owner-1" { :name "rob"})
      (database/put-index instance { 
                           :id "by_name" 
                           :filter by-name-animal-filter
                           :map by-name-map}) 
      (parse-results (database/query instance { :query "*" :index "by_name" :wait true })))))
  (it "will not index documents not covered by the filter"
      (should-not-contain { :name "rob"} @results))
  (it "will index documents covered by the filter"
      (should-contain { :name "zebra"} @results)
      (should-contain { :name "aardvark"} @results)
      (should-contain { :name "giraffe"} @results)
      (should-contain { :name "anteater"} @results)))

;; Note: Perhaps I shouldn't be doing end-to-end here
;; if it's too hard to set up a standalone indexing system
;; then I should make it easier
#_ (describe "Updating documents in the index"
  (it "will not return documents based on old data in the query"
    (with-test-server 
      (fn []
        (client/put-index 
          "http://localhost:9000" 
          "by_username" 
          "(fn [doc] {\"username\" (doc :username)})")
        (client/put-document 
          "http://localhost:9000" 
          "1" { :username "bob"})
        (should== 
          '({:username "bob"}) 
          (client/query "http://localhost:9000" 
            { :query "(= \"username\" \"bob\")" :index "by_username" :wait true}))
       (client/put-document 
          "http://localhost:9000" 
          "1" { :username "alice"})
       (should== () 
         (client/query 
          "http://localhost:9000" 
          { :query "username:bob" :index "by_username" :wait true}))))))
