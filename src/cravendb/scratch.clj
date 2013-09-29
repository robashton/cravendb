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
  "(fn [doc] (if (:whatever doc) { \"whatever\" (:whatever doc) } nil ))")

(defn add-by-whatever-index [db]
  (with-open [tx (.ensure-transaction db)] 
    (.commit! 
      (indexes/put-index tx { 
        :id "by_whatever" 
        :map test-index} )))) 

(defn add-alpha-whatevers [db]
  (with-open [tx (.ensure-transaction db)] 
    (-> tx
      (docs/store-document "docs-1" (pr-str { :whatever "zebra"}))
      (docs/store-document "docs-2" (pr-str { :whatever "aardvark"}))
      (docs/store-document "docs-3" (pr-str { :whatever "giraffe"}))
      (docs/store-document "docs-4" (pr-str { :whatever "anteater"}))
      (.commit!))))

#_ (with-full-setup
  (fn [db engine]
    (add-by-whatever-index db) 
    (add-alpha-whatevers db)
    (indexing/wait-for-index-catch-up db 50)
    (println (query/execute 
      db 
      engine 
      { :query "*:*" :sort-by "whatever" :index "by_whatever"})))) 

#_ (with-full-setup
  (fn [db engine]
    (add-alpha-whatevers db)
    (add-by-whatever-index db) 
    (println (query/execute 
      db 
      engine 
      { :query "*:*" :sort-order :desc :sort-by "whatever" :index "by_whatever"})))) 
