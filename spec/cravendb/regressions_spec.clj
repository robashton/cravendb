(ns cravendb.regressions-spec
  (:use [speclj.core]
        [cravendb.core] 
        [cravendb.testing])
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as storage]
            [cravendb.client :as client]
            [cravendb.lucene :as lucene]))

(describe "Chaser not able to complete because the last doc cannot index"
  (it "will actually catch up"
      (with-db (fn [db]

        (with-open [tx (.ensure-transaction db)]
          (.commit! (indexes/put-index tx 
            { :id "test" :map "(fn [doc] nil)"} )))

        (with-open [tx (.ensure-transaction db)]
          (-> tx
            (docs/store-document "1" (pr-str { :fod "bar" })) 
            (.commit!)))

        (with-open [ie (indexengine/create-engine db)]
          (indexing/index-documents! db (indexengine/get-compiled-indexes ie)))

        (should= (integer-to-etag 2) 
                 (indexes/get-last-indexed-etag-for-index db "test")))))) 
