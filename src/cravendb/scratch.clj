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

(defn add-all-the-test-documents [db]
  (with-open [tx (.ensure-transaction db)] 
    (.commit! (reduce  
        #(docs/store-document %1 (str "docs-" %2) (pr-str { :whatever (str %2)}))
        tx
        (range 0 1000)))))

#_ (do
  (with-full-setup
    (fn [db engine]
      (add-all-the-test-documents db)
      (with-open [tx (.ensure-transaction db)]
       (pprint (count (filter boolean (map #(docs/load-document tx (str "docs-" %1))
            (range 0 1000)))))))))

#_ (do
 (with-full-setup
  (fn [db engine]
    (add-by-whatever-index db) 
    (add-all-the-test-documents db)
    (indexing/wait-for-index-catch-up db 50)
    (count (query/execute 
             db 
             engine 
             { :query "*:*" :amount 10 :offset 0 :index "by_whatever"})))))

#_ (do
  (with-full-setup
    (fn [db engine]
      (add-by-whatever-index db) 
      (add-all-the-test-documents db)
      (indexing/wait-for-index-catch-up db 50)
      (count (query/execute 
               db 
               engine 
               { :query "*:*" :amount 10 :offset 995 :index "by_whatever"})))))
