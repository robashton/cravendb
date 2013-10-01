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
            [cravendb.lucene :as lucene]
            [instaparse.core :as insta]
            )
  (:use [cravendb.testing]
       [cravendb.core]
       [clojure.tools.logging :only (info debug error)] 
       [clojure.pprint])
  (:import 
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.store FSDirectory)
           (org.apache.lucene.store RAMDirectory)
           (org.apache.lucene.util Version)
           (org.apache.lucene.index IndexWriterConfig)
           (org.apache.lucene.index IndexWriter)
           (org.apache.lucene.index DirectoryReader)
           (org.apache.lucene.search IndexSearcher)
           (org.apache.lucene.search Sort)
           (org.apache.lucene.search SortField)
           (org.apache.lucene.search SortField$Type)
           (org.apache.lucene.search NumericRangeQuery)
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

(defn allowed-function-names)

(def query-format 
  (insta/parser
    "S = Function
    whitespace = #'\\s+'
    Function = <'('> AndCall|OrCall|EqualsCall|NotEqualsCall|ContainsCall <')'>   
    Argument = Function | LiteralValue

    AndCall = <'and'> (<whitespace> Argument)+
    OrCall = <'or'> (<whitespace> Argument )+
    EqualsCall = <'='> <whitespace> FieldName <whitespace> LiteralValue
    NotEqualsCall = <'not='> <whitespace> FieldName <whitespace> LiteralValue
    ContainsCall = <'contains'> <whitespace> FieldName <whitespace> StringValue

    LiteralValue = NumericValue | StringValue
    FieldName = #'[a-zA-Z]+'
    StringValue =  #'[a-zA-Z]+'
    NumericValue = #'[0-9]+' 
    "
  ))

#_ (query-format "(and foo)")


#_ (def dir (RAMDirectory.))
#_ (def analyzer (StandardAnalyzer. Version/LUCENE_CURRENT))
#_ (def config (IndexWriterConfig. Version/LUCENE_CURRENT analyzer))
#_ (def writer (IndexWriter. dir config))

#_ (let [doc (Document.)]
     (doseq [f (lucene/map-to-lucene { "name" "bob" "age" 27})] (.add doc f))
     (.addDocument writer doc))

#_ (.commit writer)

(.toString (NumericRangeQuery/newIntRange "age" (int 26) (int 28) true true)) 
(.toString (.parse parser "age:[26 TO 28]"))

#_ (def reader (DirectoryReader/open dir))
#_ (def searcher (IndexSearcher. reader))
#_ (def parser (QueryParser. Version/LUCENE_CURRENT "" analyzer))
#_ (def query (.parse parser "age:[26 TO 28]"))
#_ (def results (.search searcher query 100))
#_ (def results (.search searcher (NumericRangeQuery/newIntRange "age" (int 26) (int 28) true true) 100))
#_ (def docs (.scoreDocs results))
#_ (count docs)
