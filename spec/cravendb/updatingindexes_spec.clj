(ns cravendb.updatingindexes-spec
  (:use [speclj.core]
        [cravendb.core]
        [cravendb.testing]
        [cravendb.querylanguage])
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexengine :as indexengine]
            [cravendb.indexstore :as indexes]
            [cravendb.query :as query]
            [cravendb.database :as database]
            [cravendb.storage :as s]))

(def by-name-map 
  "(fn [doc] { \"name\" (:name doc) })")

(def by-bob-map 
  "(fn [doc] { \"name\" \"bob\" })")

(def by-name-animal-filter
  "(fn [doc metadata] (.startsWith (:id metadata) \"animal-\"))")

(defn add-animals [instance]
  (database/put-document instance "animal-1" { :name "zebra"} {})
  (database/put-document instance "animal-2" { :name "aardvark"} {}))

(defn add-by-bob-index [instance]
  (database/put-index instance 
                      { 
                       :id "by_name" 
                       :filter by-name-animal-filter
                       :map by-bob-map}))

(defn add-by-name-index [instance]
  (database/put-index instance
                      { 
                       :id "by_name" 
                       :filter by-name-animal-filter
                       :map by-name-map}))

(describe "handling modified indexes"
  (it "will re-index documents for a modified index"
     (with-full-setup 
      (fn [{:keys [storage index-engine] :as instance}]
         (add-animals instance)
         (add-by-bob-index instance)
         (indexing/wait-for-index-catch-up storage)
         (add-by-name-index instance)
         (should= "zebra"
          (first (map :name 
                      (database/query instance  {:filter (=? "name" "zebra") 
                                      :index "by_name" :wait true}))))))))


