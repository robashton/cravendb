(ns cravendb.indexengine-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.database :as database]
            [cravendb.storage :as s]
            [cravendb.client :as client]
            [cravendb.lucene :as lucene]))

(def index-id "by_name")
(def by-name-map "(fn [doc] { \"name\" (:name doc) })")

(defn store-test-index! [instance]
  (database/put-index instance { :id index-id :map by-name-map}))

(defn set-synctags-of-index-and-head [db index-synctag-int head-synctag-int]
  (with-open [tx (s/ensure-transaction db)]
    (-> tx
      (indexes/set-last-indexed-synctag-for-index index-id (integer-to-synctag index-synctag-int))
      (s/store (str indexing/last-indexed-synctag-key) (integer-to-synctag head-synctag-int))
      (s/commit!))))

(describe "Running index catch-ups"
  (it "will run indexes that are behind until they are caught up"
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (database/put-document instance "1" { :foo "bar" } ) 
      (database/put-document instance "2" { :foo "bas" } ) 
      (database/put-document instance "3" { :foo "baz" } )
      (indexing/wait-for-index-catch-up storage 1)
      (store-test-index! instance)
      (indexing/wait-for-index-catch-up storage index-id 1)
      (should= (integer-to-synctag 4) 
        (indexes/get-last-indexed-synctag-for-index storage index-id))))))

(describe "handling deleted indexes"
  (it "will remove deleted indexes from the collection"
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (store-test-index! instance)
      (indexing/wait-for-index-catch-up storage index-id 1)
      (database/delete-index instance index-id)
      (should-be-nil (get-in instance [:index-engine :compiled-indexes index-id]))))))

