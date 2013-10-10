(ns cravendb.indexengine-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as s]
            [cravendb.client :as client]
            [cravendb.lucene :as lucene]))

(def index-id "by_name")
(def by-name-map "(fn [doc] { \"name\" (:name doc) })")
(defn store-test-index! [db]
  (with-open [tx (s/ensure-transaction db)] 
      (-> tx
        (indexes/put-index { :id index-id :map by-name-map})
        (s/commit!))))

(defn delete-test-index! [db]
  (with-open [tx (s/ensure-transaction db)] 
      (-> tx
        (indexes/delete-index index-id )
        (s/commit!))))

(defn set-etags-of-index-and-head [db index-etag-int head-etag-int]
  (with-open [tx (s/ensure-transaction db)]
    (-> tx
      (indexes/set-last-indexed-etag-for-index index-id (integer-to-etag index-etag-int))
      (s/store (str indexing/last-indexed-etag-key) (integer-to-etag head-etag-int))
      (s/commit!))))

(describe "Deciding which indexes to execute as a chaser"
  (it "will return indexes which are behind the head"
    (with-db (fn [db]
       (set-etags-of-index-and-head db 0 10)
       (with-open [tx (s/ensure-transaction db)] 
        (should (indexengine/needs-a-new-chaser 
            {
              :db tx
              :chasers () 
            }
            {
              :id index-id 
            }
            ))))))

  (it "will not return indexes that are up to head"
    (with-db (fn [db]
      (set-etags-of-index-and-head db 10 10)
      (with-open [tx (s/ensure-transaction db)] 
        (should-not (indexengine/needs-a-new-chaser 
            {
              :db tx
              :chasers () 
            }
            {
              :id index-id 
            }
            ))))))

  (it "will not return indexes which are already running as chasers"
    (with-db (fn [db]
      (set-etags-of-index-and-head db 0 10)
      (with-open [tx (s/ensure-transaction db)] 
        (should-not (indexengine/needs-a-new-chaser 
            {
              :db tx
              :chasers [ {:id index-id }] 
            }
            {
              :id index-id 
            }
            )))))))

(describe "Deciding which indexes can be run from head"
  (with result (indexengine/indexes-which-are-up-to-date 
    {
       :chasers '({:id "one"})
       :compiled-indexes {"one" {:id "one"} "two" {:id "two"}}
    }))
  (it "will return indexes that are not chasers"
    (should-contain {:id "two"} @result))
  (it "will not return indexes that are chasers"
    (should-not-contain {:id "one"} @result)))


(describe "Running index catch-ups"
  (it "will run indexes that are behind until they are caught up"
    (with-db (fn [db]
      (with-open [ie (indexengine/create-engine db)]        
        (try 
          (indexengine/start ie)
          (with-open [tx (s/ensure-transaction db)]
            (-> tx
              (docs/store-document "1" (pr-str { :foo "bar" }) ) 
              (docs/store-document "2" (pr-str { :foo "bas" }) ) 
              (docs/store-document "3" (pr-str { :foo "baz" }) )
              (s/commit!)))
          (indexing/wait-for-index-catch-up db 1)
          (store-test-index! db)
          (indexing/wait-for-index-catch-up db index-id 1)
          (should= (integer-to-etag 4) 
                  (indexes/get-last-indexed-etag-for-index db index-id))
          (finally (indexengine/stop ie)))))))) 


(describe "handling deleted indexes"
  (it "will remove deleted indexes from the collection"
    (with-open [db (s/create-storage "testdir")
              engine (indexengine/create-engine db)]
      (store-test-index! db)
      (let [state-with-index (indexengine/refresh-indexes! @(:ea engine))]
        (delete-test-index! db)
        (let [state-without-index (indexengine/refresh-indexes! state-with-index)]
          (should== ["default"] (map key (:compiled-indexes state-without-index)))))))) 

