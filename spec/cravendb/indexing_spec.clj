(ns cravendb.indexing-spec
  (:use [speclj.core]
        [cravendb.testing])
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as storage]
            [cravendb.client :as client]
            [cravendb.lucene :as lucene]))

(defn create-test-index []  
  (let [storage (lucene/create-memory-index)]
                    {
                    :id "test" 
                    :map (fn [doc] {"author" (doc :author)})
                    :storage storage
                    :writer (.open-writer storage) }))

(defn create-test-indexes [] [ (create-test-index) ])

(def write-three-documents 
  (fn [db]
    (with-open [tx (.ensure-transaction db)]
      (-> tx
        (docs/store-document "1" (pr-str { :title "hello" :author "rob"}))
        (docs/store-document "2" (pr-str { :title "morning" :author "vicky"}))
        (docs/store-document "3" (pr-str { :title "goodbye" :author "james"}))
        (.commit!)))))

(def write-one-document 
  (fn [db]
    (with-open [tx (.ensure-transaction db)]
      (-> tx
        (docs/store-document "2" (pr-str { :title "morning" :author "vicky"}))
        (.commit!)))))

(describe "indexing some documents"
  (with test-indexes (create-test-indexes))
  (it "will index all the documents"
    (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db @test-indexes)
        (with-open [tx (.ensure-transaction db)]
          (should= 3 (indexing/last-index-doc-count tx))
          (should= (docs/last-etag tx) (indexing/last-indexed-etag tx)))))))

(describe "Repeated indexing with no new documents"
  (with test-indexes (create-test-indexes))
  (it "will not try to index documents twice"
    (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db @test-indexes)
        (indexing/index-documents! db @test-indexes)
        (with-open [tx (.ensure-transaction db)]
          (should= 0 (indexing/last-index-doc-count tx))
          (should= (docs/last-etag tx) (indexing/last-indexed-etag tx)))))))

(describe "indexing new documents"
  (with test-indexes (create-test-indexes))
  (it "will index only the new documents"
    (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db @test-indexes)
        (write-one-document db)
        (indexing/index-documents! db @test-indexes)
        (with-open [tx (.ensure-transaction db)]
          (should= (docs/last-etag tx) (indexing/last-indexed-etag tx))
          (should= 1 (indexing/last-index-doc-count tx)))))))

(describe "loading indexes from the database and querying using them"
  (with test-indexes (create-test-indexes))
  (it "will index all the documents"
    (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db @test-indexes)
        (with-open [reader (.open-reader ((first @test-indexes) :storage))]
            (should== '("2") (.query reader { :query "author:vicky"})))))))

(describe "querying an index with content in it"
  (with test-indexes (create-test-indexes))
  (it "will return the right document ids"
    (with-db (fn [db]
        (with-open [tx (.ensure-transaction db)]
          (.commit! 
            (indexes/put-index tx 
                { :id "by_author" :map "(fn [doc] {\"author\" (doc :author)})"})))
        (write-three-documents db)

        (let [ie (indexengine/load-from db)]
          (try
            (indexing/index-documents! db (:compiled-indexes ie))
            (finally
              indexengine/close-engine ie)))

        (with-open [tx (.ensure-transaction db)]
          (should= 4 (indexing/last-index-doc-count tx)) ;; The index counts
          (should= (docs/last-etag tx) (indexing/last-indexed-etag tx)))))))

(describe "Running indexing with no documents or indexes"
  (it "will not fall over in a heap, crying with a bottle of whisky"
    (with-db (fn [db]
      (let [ie (indexengine/load-from db)]
        (try
          (should-not-throw (indexing/index-documents! db (:compiled-indexes ie)))
          (finally
            (indexengine/close-engine ie))))))))

(describe "Updating documents in the index"
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
            { :query "username:bob" :index "by_username" :wait true}))
       (client/put-document 
          "http://localhost:9000" 
          "1" { :username "alice"})
       (should== () 
         (client/query 
          "http://localhost:9000" 
          { :query "username:bob" :index "by_username" :wait true}))))))
