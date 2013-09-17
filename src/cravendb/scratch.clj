(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexes :as indexes]
            [cravendb.storage :as storage]
            [me.raynes.fs :as fs]
            [cravendb.lucene :as lucene]))



#_ (def db (storage/create-storage "testdb"))
#_ (.close db)
#_ (fs/delete-dir "testdb")

#_  (with-open [tx (.ensure-transaction db)]
      (-> tx
        (docs/store-document "1" (pr-str { :title "hello" :author "rob"}))
        (docs/store-document "2" (pr-str { :title "morning" :author "vicky"}))
        (docs/store-document "3" (pr-str { :title "goodbye" :author "james"}))
        (.commit!)))  


#_ (def test-index {:id "test" :map (fn [doc] {"author" (doc :author)}) :storage (lucene/create-memory-index) })
#_ (def test-indexes [ test-index ])

#_ (indexing/index-documents! db test-indexes)

