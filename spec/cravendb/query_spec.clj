(ns cravendb.query-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])

  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as storage]
            [cravendb.client :as client]
            [cravendb.query :as query]
            [cravendb.lucene :as lucene]))


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

(describe "paging like a boss"
  (it "will return the first 10 docs"
    (with-full-setup
    (fn [db engine]
      (add-by-whatever-index db) 
      (add-all-the-test-documents db)
      (indexing/wait-for-index-catch-up db 50)
      (should= 10 (count (query/execute 
                        db 
                        engine 
                        { :query "*:*" :amount 10 :offset 0 :index "by_whatever"}))))))

   (it "will return the last 5 docs"
    (with-full-setup
    (fn [db engine]
      (add-by-whatever-index db) 
      (add-all-the-test-documents db)
      (indexing/wait-for-index-catch-up db 50)
      (should= 5 (count (query/execute 
                        db 
                        engine 
                        { :query "*:*" :amount 10 :offset 995 :index "by_whatever"})))))))
