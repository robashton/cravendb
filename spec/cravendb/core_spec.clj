(ns cravendb.documents-spec
  (:use [speclj.core])
  (require [cravendb.documents :as documents]
           [me.raynes.fs :as fs]))

(describe "Opening a database"
  (before
    (fs/delete-dir "testdir")
    (.close (documents/opendb "testdir")))
  (after
    (fs/delete-dir "testdir"))
  (it "Should create a goddamned directory"
    (should (fs/directory? "testdir"))))

