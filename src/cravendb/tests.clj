(ns cravendb.tests
    (:use clojure.test)
    (require [cravendb.documents :as documents]))

(deftest test-open
  (let [db (documents/opendb "test1")]
    (.close db)))

(run-tests)
