(ns cravendb.storage-spec
  (:use [speclj.core])
  (:require [cravendb.storage :as storage]))

(describe "Opening a database"
  (before
    (fs/delete-dir "testdir")
    (.close (db/open-db "testdir")))
  (after
    (fs/delete-dir "testdir"))
  (it "Should create a goddamned directory"
    (should (fs/directory? "testdir"))))

