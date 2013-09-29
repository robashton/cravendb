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


(def test-index 
  "(fn [doc] { \"name\" (:name doc) })")

(def test-index-filter
  "(fn [doc metadata] (.startsWith (:id metadata) \"animal-\"))")

(defn add-alpha-whatevers [db]
  (with-open [tx (.ensure-transaction db)] 
    (-> tx
      (docs/store-document "animal-1" (pr-str { :name "zebra"}))
      (docs/store-document "animal-2" (pr-str { :name "aardvark"}))
      (docs/store-document "animal-3" (pr-str { :name "giraffe"}))
      (docs/store-document "animal-4" (pr-str { :name "anteater"}))
      (docs/store-document "owner-1" (pr-str { :name "rob"}))
      (.commit!))))

(defn add-by-whatever-index [db]
  (with-open [tx (.ensure-transaction db)] 
    (.commit! 
      (indexes/put-index tx { 
        :id "by_name" 
        :filter test-index-filter
        :map test-index} )))) 

#_ (with-full-setup
  (fn [db engine]
    (add-by-whatever-index db) 
    (add-alpha-whatevers db)
    (pprint (query/execute 
      db 
      engine 
      { :query 
       "*:*" 
       :index "by_name"
       :wait true
       })))) 
