(ns cravendb.query-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])

  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as s]
            [cravendb.client :as client]
            [cravendb.query :as query]
            [cravendb.lucene :as lucene]))


(def test-index 
  "(fn [doc] (if (:whatever doc) { \"whatever\" (:whatever doc) } nil ))")

(defn add-by-whatever-index [db]
  (with-open [tx (s/ensure-transaction db)] 
    (s/commit! 
      (indexes/put-index tx { 
        :id "by_whatever" 
        :map test-index} )))) 

(defn add-1000-documents [db]
  (with-open [tx (s/ensure-transaction db)] 
    (s/commit! (reduce  
        #(docs/store-document %1 (str "docs-" %2) (pr-str { :whatever (str %2)}))
        tx
        (range 0 1000)))))

(defn add-alpha-whatevers [db]
  (with-open [tx (s/ensure-transaction db)] 
    (-> tx
      (docs/store-document "docs-1" (pr-str { :whatever "zebra"}))
      (docs/store-document "docs-2" (pr-str { :whatever "aardvark"}))
      (docs/store-document "docs-3" (pr-str { :whatever "giraffe"}))
      (docs/store-document "docs-4" (pr-str { :whatever "anteater"}))
      (s/commit!))))

(describe "paging like a boss"
  (it "will return the first 10 docs"
    (with-full-setup
    (fn [db engine]
      (add-by-whatever-index db) 
      (add-1000-documents db)
      (indexing/wait-for-index-catch-up db 50)
      (should== (map str (range 0 10)) 
                (map (comp :whatever read-string) 
                     (query/execute 
                      db 
                      engine 
                      { :query "*:*" :amount 10 :offset 0 :index "by_whatever"}))))))

   (it "will return the last 5 docs"
    (with-full-setup
    (fn [db engine]
      (add-by-whatever-index db) 
      (add-1000-documents db)
      (indexing/wait-for-index-catch-up db 50)
      (should== (map str (range 995 1000)) 
                (map (comp :whatever read-string) 
                     (query/execute 
                      db 
                      engine 
                      { :query "*:*" :amount 10 :offset 995 :index "by_whatever"})))))))

(describe "sorting"
  (it "will default to ascending order on a string"
    (with-full-setup
      (fn [db engine]
        (add-by-whatever-index db) 
        (add-alpha-whatevers db)
        (indexing/wait-for-index-catch-up db 50)
        (should== ["aardvark" "anteater" "giraffe" "zebra"]
          (map 
            (comp :whatever read-string) 
            (query/execute 
              db 
              engine 
              { :query "*:*" :sort-by "whatever" :index "by_whatever"}))))) 
              )
  (it "will accept descending order on a string"
    (with-full-setup
      (fn [db engine]
        (add-by-whatever-index db) 
        (add-alpha-whatevers db)
        (indexing/wait-for-index-catch-up db 50)
        (should== [ "zebra" "giraffe" "anteater" "aardvark"]
          (map 
            (comp :whatever read-string) 
            (query/execute 
              db 
              engine 
              { :query "*:*" :sort-order :desc :sort-by "whatever" :index "by_whatever"}))))) 
              ))


