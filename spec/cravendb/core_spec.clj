(ns cravendb.documents-spec
  (:use [speclj.core])
  (:require [cravendb.documents :as documents]
            [cravendb.leveldb :as db]
           [me.raynes.fs :as fs]))

(describe "Opening a database"
  (before
    (fs/delete-dir "testdir")
    (.close (db/open-db "testdir")))
  (after
    (fs/delete-dir "testdir"))
  (it "Should create a goddamned directory"
    (should (fs/directory? "testdir"))))

