(ns cravendb.tests
    (:use clojure.test)
    (require [cravendb.documents :as documents]
             [me.raynes.fs :as fs]))

(deftest test-open-db
  (fs/delete-dir "testdir")
  (.close (documents/opendb "testdir"))
  (is (fs/directory? "testdir"))
  (fs/delete-dir "testdir"))



(run-tests)
