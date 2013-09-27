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


#_ (with-open [db (storage/create-storage "testdir")
               engine (indexengine/create-engine db)
               reader (.open-reader engine "by_whatever")]
      (println (map (comp :whatever read-string)  (query/execute db engine { :query "*:*" :amount 10 :offset 0 :index "by_whatever"})))
     
      (println (map (comp :whatever read-string)  (query/execute db engine { :query "*:*" :amount 10 :offset 10 :index "by_whatever"}))) 

      (println (map (comp :whatever read-string)  (query/execute db engine { :query "*:*" :amount 500 :offset 250 :index "by_whatever"}))) 
     )


#_ (with-open [db (storage/create-storage "testdir")
               engine (indexengine/create-engine db)]
      (try       
        (.start engine)

        (with-open [tx (.ensure-transaction db)] 
          (.commit! 
            (indexes/put-index tx { 
              :id "by_whatever" 
              :map "(fn [doc] (if (:whatever doc) { \"whatever\" (:whatever doc) } nil ))"} )))

        (with-open [tx (.ensure-transaction db)] 
          (.commit! 
            (reduce  
              (fn [tx i] 
                (docs/store-document 
                  tx 
                  (str "docs-" i) 
                  (pr-str { :whatever (str i)})))
              tx
              (range 0 1000))))

        (indexing/wait-for-index-catch-up db 50)

        (println (map (comp :whatever read-string)  (query/execute db engine { :query "*:*" :amount 10 :offset 0 :index "by_whatever"})))))

   ;;     (println (map (comp :whatever read-string)  (query/execute db engine { :query "*:*" :amount 10 :offset 10 :index "by_whatever"})))



