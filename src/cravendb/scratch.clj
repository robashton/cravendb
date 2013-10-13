(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        )

  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [me.raynes.fs :as fs]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as s]
            [cravendb.client :as client]
            [cravendb.query :as query]
            [cravendb.database :as db]
            [cravendb.lucene :as lucene]))



#_ (def instance 
     (do
       (fs/delete-dir "testdb")
        (db/create "testdb")))

#_ (db/put-document instance "test1", (pr-str { :foo "baz"}))

#_ (def first-etag (docs/etag-for-doc (:storage instance) "test1"))

#_ (db/put-document instance "test1", (pr-str { :foo "baz"}) first-etag)
#_ (docs/conflicts (:storage instance))

#_ (.close instance)

(defn is-conflict [session id current-etag]
  (and current-etag (not= current-etag (docs/etag-for-doc session id))))


#_ (is-conflict (:storage instance) "test1" first-etag)
