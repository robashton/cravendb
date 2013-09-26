(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.client :as client]
            [cravendb.query :as query]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as storage]
            [me.raynes.fs :as fs]
            [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.http :as http]  
            [cravendb.lucene :as lucene])
  (use [cravendb.testing]
       [cravendb.core]
       [clojure.tools.logging :only (info debug error)] 
       [clojure.pprint]))


#_ (defn create-test-index []  
  (let [storage (lucene/create-memory-index)]
                    {
                    :id "test" 
                    :map (fn [doc] {"author" (doc :author)})
                    :storage storage
                    :writer (.open-writer storage) }))

#_ (defn create-test-indexes [] [ (create-test-index) ])

#_ (def write-three-documents 
  (fn [db]
    (with-open [tx (.ensure-transaction db)]
      (-> tx
        (docs/store-document "doc-1" (pr-str { :title "hello" :author "rob"}))
        (docs/store-document "doc-2" (pr-str { :title "morning" :author "vicky"}))
        (docs/store-document "doc-3" (pr-str { :title "goodbye" :author "james"}))
        (.commit!)))))

#_ (def test-indexes (create-test-indexes))

#_    (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db test-indexes)
        (with-open [reader (.open-reader ((first test-indexes) :storage))]
          (println (.query reader { :query "author:vicky"})))))  

