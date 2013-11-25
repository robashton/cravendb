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
            [cravendb.database :as db]
            [cravendb.lucene :as lucene]))


(def test-index 
  "(fn [doc] (if (:whatever doc) { \"whatever\" (:whatever doc) } nil ))")

(defn add-by-whatever-index [instance]
  (db/put-index instance { 
                         :id "by_whatever" 
                         :map test-index})) 

(defn add-100-documents [instance]
  (db/bulk instance
    (map (fn [i]
           {
            :operation :docs-put
            :id (str "docs-" i)
            :document { :whatever (str i)} 
            }) (range 0 100))))

(defn add-alpha-whatevers [instance]
  (db/put-document instance "docs-1" { :whatever "zebra"} {})
  (db/put-document instance "docs-2" { :whatever "aardvark"} {})
  (db/put-document instance "docs-3" { :whatever "giraffe"} {})
  (db/put-document instance "docs-4" { :whatever "anteater"} {}))

(describe "paging like a boss"
  (multi "will return the first 10 docs"
  (add-by-whatever-index instance) 
  (add-100-documents instance)
  (should== (map str (range 0 10)) 
            (map :whatever 
              (db/query instance
                {:wait true :filter "*" :amount 10 :offset 0 :index "by_whatever"}))))

   (multi "will return the last 5 docs"
    (add-by-whatever-index instance) 
    (add-100-documents instance)
    (should== (map str (range 95 100)) 
              (map :whatever 
              (db/query instance 
                {:wait true :filter "*" :amount 10 :offset 95 :index "by_whatever"})))))

(describe "sorting like a boss"
  (multi "will default to ascending order on a string"
    (add-by-whatever-index instance) 
    (add-alpha-whatevers instance)
    (should== ["aardvark" "anteater" "giraffe" "zebra"]
              (map :whatever 
                  (db/query instance
                            {:wait true :filter "*" :sort-order :asc :sort-by "whatever" :index "by_whatever"}))))

  (multi "will accept descending order on a string"
    (add-by-whatever-index instance) 
    (add-alpha-whatevers instance)
    (should== [ "zebra" "giraffe" "anteater" "aardvark"]
              (map :whatever 
                (db/query instance 
                {:wait true :filter "*" :sort-order :desc :sort-by "whatever" :index "by_whatever"})))))
