(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.client :as client]
            [cravendb.query :as query]
            [cravendb.indexstore :as indexes]
            [cravendb.queryparsing :as qp]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as storage]
            [cravendb.storage :as s]
            [me.raynes.fs :as fs]
            [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.http :as http]  
            [cravendb.lucene :as lucene]
            [instaparse.core :as insta])
  (:use [cravendb.testing]
       [cravendb.core]
       [clojure.tools.logging :only (info debug error)] 
       [clojure.pprint]
       [cravendb.querylanguage]
        )
  (:import 
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.store FSDirectory)
           (org.apache.lucene.store RAMDirectory)
           (org.apache.lucene.util Version)
           (org.apache.lucene.index IndexWriterConfig)
           (org.apache.lucene.index IndexWriter)
           (org.apache.lucene.index DirectoryReader)
           (org.apache.lucene.index Term)
           (org.apache.lucene.search IndexSearcher)
           (org.apache.lucene.search Sort)
           (org.apache.lucene.search SortField)
           (org.apache.lucene.search SortField$Type)
           (org.apache.lucene.search NumericRangeQuery)
           (org.apache.lucene.search TermQuery)
           (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.document Document)
           (org.apache.lucene.document Field)
           (org.apache.lucene.document Field$Store)
           (org.apache.lucene.document Field$Index)
           (org.apache.lucene.document TextField)
           (org.apache.lucene.document StringField)
           (org.apache.lucene.document IntField)
           (org.apache.lucene.document FloatField)
           (java.util Collection Random)
           (java.io File File PushbackReader IOException FileNotFoundException )))


#_ (qp/to-lucene "(= \"foo\" 2)") 
#_ (qp/to-lucene "(= \"foo\" \"blah\")") 
#_ (qp/to-lucene "(starts-with \"foo\" \"blah\")") 


#_ (def dir (RAMDirectory.))
#_ (def analyzer (StandardAnalyzer. Version/LUCENE_CURRENT))
#_ (def config (IndexWriterConfig. Version/LUCENE_CURRENT analyzer))
#_ (def writer (IndexWriter. dir config))

#_ (let [doc (Document.)]
     (doseq [f (lucene/map-to-lucene { "name" "bob" "age" 27})] (.add doc f))
     (.addDocument writer doc))

#_ (.commit writer)

#_ (.toString (NumericRangeQuery/newIntRange "age" (in -100000) (int 28) true true)) 
#_ (.toString (.parse parser "age:[26 TO *]"))

#_ (def reader (DirectoryReader/open dir))
#_ (def searcher (IndexSearcher. reader))
#_ (def parser (QueryParser. Version/LUCENE_CURRENT "" analyzer))
#_ (def query (.parse parser "Vlah:egw*"))
#_ (def results (.search searcher query 100))
#_ (def results (.search searcher (NumericRangeQuery/newIntRange "age" (int 26) (int 28) true true) 100))
#_ (def results (.search searcher (first (drop 1 (query-to-lucene (query-format "(= \"age\" 27)")))) 100))

#_ (def docs (.scoreDocs results))


(defn create-test-index []  
  (let [storage (lucene/create-memory-index)]
                    {
                    :id "test" 
                    :map (fn [doc] {"hello" ((:blah doc) :foo)})
                    :storage storage
                    :writer (lucene/open-writer storage) }))

(defn create-valid-index []  
  (let [storage (lucene/create-memory-index)]
                    {
                    :id "valid" 
                     :map (fn [doc] {"hello" (:author doc)})
                    :storage storage
                    :writer (lucene/open-writer storage) }))

(defn create-test-indexes [] [ (create-test-index) (create-valid-index) ])


(def write-three-documents 
  (fn [db]
    (with-open [tx (s/ensure-transaction db)]
      (-> tx
        (docs/store-document "doc-1" (pr-str { :title "hello" :author "rob"}))
        (docs/store-document "doc-2" (pr-str { :title "morning" :author "vicky"}))
        (docs/store-document "doc-3" (pr-str { :title "goodbye" :author "james"}))
        (s/commit!)))))

;; Excellent
#_ (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db (create-test-indexes))
        (println (indexes/is-failed db "test"))
        (println (indexes/errors db "test"))))

;; Even better
#_ (with-db (fn [db]
        (write-three-documents db)
        (indexing/index-documents! db (create-test-indexes))
        (indexing/index-documents! db (create-test-indexes))
        (with-open [tx (s/ensure-transaction db)]
          (s/commit! (indexes/reset-index tx "test")))
        (println (indexes/is-failed db "test"))
        (println (indexes/get-last-indexed-etag-for-index db "test"))))



