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

#_ (with-open [tx (s/ensure-transaction (:storage instance))]
     (s/commit! (docs/without-conflicts tx "test1")))

#_ (.close instance)


;; I want to append conflicts to the list 
;; If I mark the conflict as resolved, specifying an e-tag, I'll use that document to blow all conflicts away
;; -> Default behaviour of last-write wins as a conflict resolution algorithm - but that would imply shared etag constructs across cluster
;; Deleting a document can also cause conflict
;; Instead of treating them as conflicts, let's call them "failed writes"
