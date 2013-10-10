(ns cravendb.updating-indexes-spec.clj
  (:use [speclj.core]
        [cravendb.core]
        [cravendb.testing]
        [cravendb.querylanguage])
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexengine :as indexengine]
            [cravendb.indexstore :as indexes]
            [cravendb.query :as query]
            [cravendb.storage :as s]))

(def by-name-map 
  "(fn [doc] { \"name\" (:name doc) })")

(def by-bob-map 
  "(fn [doc] { \"name\" \"bob\" })")

(def by-name-animal-filter
  "(fn [doc metadata] (.startsWith (:id metadata) \"animal-\"))")

(defn add-animals [db]
  (with-open [tx (s/ensure-transaction db)] 
          (-> tx
            (docs/store-document "animal-1" (pr-str { :name "zebra"}))
            (docs/store-document "animal-2" (pr-str { :name "aardvark"}))
            (s/commit!))))

(defn add-by-bob-index [db]
  (with-open [tx (s/ensure-transaction db)] 
    (-> tx
      (indexes/put-index { 
        :id "by_name" 
        :filter by-name-animal-filter
        :map by-bob-map})
      (s/commit!))))

(defn add-by-name-index [db]
  (with-open [tx (s/ensure-transaction db)] 
    (-> tx
      (indexes/put-index { 
        :id "by_name" 
        :filter by-name-animal-filter
        :map by-name-map})
      (s/commit!))))

(describe "handling modified indexes"
  (it "will reset the etag of a modified index"
      (with-full-setup 
       (fn [db engine]
         (add-animals db)
         (add-by-bob-index db)
         (indexing/wait-for-index-catch-up db)
         (add-by-name-index db)
         (should= 0 (etag-to-integer
                      (indexes/get-last-indexed-etag-for-index db "by_name"))))))
  (it "will re-index documents for a modified index"
     (with-full-setup 
       (fn [db engine]
         (add-animals db)
         (add-by-bob-index db)
         (indexing/wait-for-index-catch-up db)
         (add-by-name-index db)
         (should= "zebra"
            (first (map (comp :name read-string) 
                (query/execute db engine { :query (=? "name" "zebra") 
                              :index "by_name" :wait true}))))))))


