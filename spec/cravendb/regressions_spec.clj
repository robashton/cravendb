(ns cravendb.regressions-spec
  (:use [speclj.core]
        [cravendb.core] 
        [cravendb.testing])
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as s]
            [cravendb.query :as query]
            [cravendb.client :as client]
            [cravendb.lucene :as lucene]))

(describe "Chaser not able to complete because the last doc cannot index"
  (it "will actually catch up"
      (with-db (fn [db]

        (with-open [tx (s/ensure-transaction db)]
          (s/commit! (indexes/put-index tx 
            { :id "test" :map "(fn [doc] nil)"} )))

        (with-open [tx (s/ensure-transaction db)]
          (-> tx
            (docs/store-document "1" (pr-str { :fod "bar" })) 
            (s/commit!)))

        (with-open [ie (indexengine/create-engine db)]
          (indexing/index-documents! db (indexengine/get-compiled-indexes ie)))

        (should= (integer-to-etag 2) 
                 (indexes/get-last-indexed-etag-for-index db "test")))))) 

(def test-index 
  "(fn [doc] (if (:whatever doc) { \"whatever\" (:whatever doc) } nil ))")

(defn add-by-whatever-index [db]
  (with-open [tx (s/ensure-transaction db)] 
    (s/commit! 
      (indexes/put-index tx { 
        :id "by_whatever" 
        :map test-index} )))) 

(describe "Querying a newly created index"
  (it "will not fall over clutching a bottle of whisky"
    (with-full-setup
    (fn [db engine]
      (add-by-whatever-index db) 
      (should-not-throw 
        (query/execute 
          db 
          engine 
          { :query "*:*" :sort-order :desc :sort-by "whatever" :index "by_whatever"}))))))
 
