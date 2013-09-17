(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.client :as client]
            [cravendb.query :as query]
            [cravendb.indexes :as indexes]
            [cravendb.storage :as storage]
            [me.raynes.fs :as fs]
            [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.http :as http]  
            [cravendb.lucene :as lucene]))



#_ (def db (indexes/load-compiled-indexes (storage/create-storage "testdb")))
#_ (.close db)
#_ (fs/delete-dir "testdb")


#_ (def server (run-jetty (http/create-http-server db) { :port 9000 :join? false}))
#_ (.stop server)


#_ (with-open [tx (.ensure-transaction db)]
      (-> tx
        (docs/store-document "1" (pr-str { :title "hello" :author "rob"}))
        (docs/store-document "2" (pr-str { :title "morning" :author "vicky"}))
        (docs/store-document "3" (pr-str { :title "goodbye" :author "james"}))
        (.commit!)))  


#_ (def test-index {:id "test" :map (fn [doc] {"author" (doc :author)}) :storage (lucene/create-memory-index) })
#_ (def test-indexes [ test-index ])

#_ (indexing/index-documents! db test-indexes)
#_ (with-open [tx (.ensure-transaction db)]
     (with-open [reader (.open-reader ((first test-indexes) :storage))]
       (doall (map (partial docs/load-document tx) (.query reader { :query "author:vicky"})))))


#_  (with-open [tx (.ensure-transaction db)]
      (query/execute tx { :query "author:vicky"}))

#_ (client/get-document "http://localhost:9000" "1")
#_ (client/query "http://localhost:9000" {
                                          :query "author:vicky"
                                          })
