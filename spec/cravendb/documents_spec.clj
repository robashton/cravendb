(ns cravendb.documents-spec
  (:use [speclj.core])
  (require 
    [cravendb.documents :as documents]
    [me.raynes.fs :as fs]))

(describe "Various db operations"
  (before 
    (fs/delete-dir "testdir"))
  (after 
    (fs/delete-dir "testdir"))
  (it "should be able to open the db"
    (should true)))

