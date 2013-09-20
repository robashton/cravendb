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
            [cravendb.lucene :as lucene]))


#_ (def db (indexengine/start (storage/create-storage "testdb3")))


#_ (with-open [tx (.ensure-transaction db)]
      (-> tx
        (docs/store-document "1" (pr-str { :title "hello" :author "rob"}))
        (docs/store-document "2" (pr-str { :title "morning" :author "vicky"}))
        (docs/store-document "3" (pr-str { :title "goodbye" :author "james"}))
        (.commit!)))  

#_ (with-open [tx (.ensure-transaction db)]
     (-> tx
       (indexes/put-index { :id "by_author" :map "(fn [doc] {\"author\" (doc :author)})"})
       (.commit!)))

#_ (def server 
     (run-jetty 
       (http/create-http-server db) { :port 9001 :join? false}))

#_ (indexengine/teardown db) 
#_ (.stop server)
#_ (.close db)
#_ (fs/delete-dir "testdb3")

#_ (def test-index (let [storage (lucene/create-memory-index)]
                    {
                    :id "test" 
                    :map (fn [doc] {"author" (doc :author)})
                    :storage storage
                    :writer (.open-writer storage) }))

#_ (def test-indexes [ test-index ])

#_  (with-open [tx (.ensure-transaction db)]
      (query/execute tx loaded-indexes { :index "by_author" :query "author:vicky"}))

#_ (client/get-document "http://localhost:9001" "1")

#_ (client/query "http://localhost:9001" {
                                          :index "by_author"
                                          :query "author:vicky"
                                          })
