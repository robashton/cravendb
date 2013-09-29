(ns cravendb.indexing-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])

  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexstore :as indexes]
            [cravendb.query :as query]
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

(defn parse-results [results]
  (map read-string results))

(def write-three-documents 
  (fn [db]
    (with-open [tx (.ensure-transaction db)]
      (-> tx
        (docs/store-document "doc-1" (pr-str { :title "hello" :author "rob"}))
        (docs/store-document "doc-2" (pr-str { :title "morning" :author "vicky"}))
        (docs/store-document "doc-3" (pr-str { :title "goodbye" :author "james"}))
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
            (should== '("doc-2") (.query reader "author:vicky" 10 nil nil)))))))

(describe "querying an index with content in it"
  (with test-indexes (create-test-indexes))
  (it "will return the right document ids"
    (with-db (fn [db]
        (with-open [tx (.ensure-transaction db)]
          (.commit! 
            (indexes/put-index tx 
                { :id "by_author" :map "(fn [doc] {\"author\" (doc :author)})"})))

        (write-three-documents db)

        (with-open [ie (indexengine/create-engine db)]
          (indexing/index-documents! db (indexengine/get-compiled-indexes ie)))

        (with-open [tx (.ensure-transaction db)]
          (should= 4 (indexing/last-index-doc-count tx)) ;; The index counts
          (should= (docs/last-etag tx) (indexing/last-indexed-etag tx)))))))

(describe "Running indexing with no documents or indexes"
  (it "will not fall over in a heap, crying with a bottle of whisky"
    (with-db (fn [db]
      (with-open [ie (indexengine/create-engine db)]
        (should-not-throw (indexing/index-documents! db (indexengine/get-compiled-indexes ie))))))))


(describe "Keeping track of per index status"
  (it "will start each tracker off at zero status"
      (with-db (fn [db]
        (with-open [tx (.ensure-transaction db)]
          (.commit! (indexes/put-index tx { :id "test" } )))
        
        (should= (integer-to-etag 0) 
                 (indexes/get-last-indexed-etag-for-index db "test")))))

  (it "will set the tracker to the last indexed etag"
      (with-db (fn [db]

        (with-open [tx (.ensure-transaction db)]
          (.commit! (indexes/put-index tx 
            { :id "test" :map "(fn [doc] {\"foo\" (:foo doc)})"} )))

        (with-open [tx (.ensure-transaction db)]
          (-> tx
            (docs/store-document "1" (pr-str { :foo "bar" }) ) 
            (docs/store-document "2" (pr-str { :foo "bas" }) ) 
            (docs/store-document "3" (pr-str { :foo "baz" }) )
            (.commit!)))

        (with-open [ie (indexengine/create-engine db)]
          (indexing/index-documents! db (indexengine/get-compiled-indexes ie)))

        (should= (integer-to-etag 4) 
                 (indexes/get-last-indexed-etag-for-index db "test")))))) 

(def by-name-map 
  "(fn [doc] { \"name\" (:name doc) })")

(def by-name-animal-filter
  "(fn [doc metadata] (.startsWith (:id metadata) \"animal-\"))")

(describe "Applying a filter to an index"
  (with-all results (with-full-setup
      (fn [db engine]
        (with-open [tx (.ensure-transaction db)] 
          (-> tx
            (docs/store-document "animal-1" (pr-str { :name "zebra"}))
            (docs/store-document "animal-2" (pr-str { :name "aardvark"}))
            (docs/store-document "animal-3" (pr-str { :name "giraffe"}))
            (docs/store-document "animal-4" (pr-str { :name "anteater"}))
            (docs/store-document "owner-1" (pr-str { :name "rob"}))
            (indexes/put-index { 
              :id "by_name" 
              :filter by-name-animal-filter
              :map by-name-map}) 
            (.commit!)))
        (parse-results
          (query/execute db engine { :query "*:*" :index "by_name" :wait true })))))
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
            { :query "username:bob" :index "by_username" :wait true}))
       (client/put-document 
          "http://localhost:9000" 
          "1" { :username "alice"})
       (should== () 
         (client/query 
          "http://localhost:9000" 
          { :query "username:bob" :index "by_username" :wait true}))))))
