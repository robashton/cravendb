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
            [cravendb.database :as database]
            [cravendb.lucene :as lucene]))


(def test-index 
  "(fn [doc] (if (:whatever doc) { \"whatever\" (:whatever doc) } nil ))")

(defn add-by-whatever-index [instance]
  (database/put-index instance { 
                         :id "by_whatever" 
                         :map test-index})) 

(defn add-1000-documents [instance]
  (database/bulk instance
    (map (fn [i]
           {
            :operation :docs-put
            :id (str "docs-" i)
            :document (pr-str { :whatever (str i)})
            }) (range 0 1000))))

(defn add-alpha-whatevers [instance]
  (database/put-document instance "docs-1" (pr-str { :whatever "zebra"}))
  (database/put-document instance "docs-2" (pr-str { :whatever "aardvark"}))
  (database/put-document instance "docs-3" (pr-str { :whatever "giraffe"}))
  (database/put-document instance "docs-4" (pr-str { :whatever "anteater"})))

(describe "paging like a boss"
  (it "will return the first 10 docs"
    (with-full-setup
    (fn [{:keys [storage index-engine] :as instance}]
      (add-by-whatever-index instance) 
      (add-1000-documents instance)
      (indexing/wait-for-index-catch-up storage 50)
      (should== (map str (range 0 10)) 
                (map (comp :whatever read-string) 
                     (database/query instance
                      { :query "*" :amount 10 :offset 0 :index "by_whatever"}))))))

   (it "will return the last 5 docs"
    (with-full-setup
    (fn [{:keys [storage index-engine] :as instance}]
      (add-by-whatever-index instance) 
      (add-1000-documents instance)
      (indexing/wait-for-index-catch-up storage 50)
      (should== (map str (range 995 1000)) 
                (map (comp :whatever read-string) 
                     (database/query instance 
                      { :query "*" :amount 10 :offset 995 :index "by_whatever"})))))))

(describe "sorting"
  (it "will default to ascending order on a string"
    (with-full-setup
      (fn [{:keys [storage index-engine] :as instance}]
        (add-by-whatever-index instance) 
        (add-alpha-whatevers instance)
        (indexing/wait-for-index-catch-up storage 50)
        (should== ["aardvark" "anteater" "giraffe" "zebra"]
          (map 
            (comp :whatever read-string) 
            (database/query instance
              { :query "*" :sort-by "whatever" :index "by_whatever"}))))))

  (it "will accept descending order on a string"
    (with-full-setup
      (fn [{:keys [storage index-engine] :as instance}]
        (add-by-whatever-index instance) 
        (add-alpha-whatevers instance)
        (indexing/wait-for-index-catch-up storage 50)
        (should== [ "zebra" "giraffe" "anteater" "aardvark"]
          (map 
            (comp :whatever read-string) 
            (database/query instance
              { :query "*" :sort-order :desc :sort-by "whatever" :index "by_whatever"})))))))
