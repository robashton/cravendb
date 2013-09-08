(ns cravendb.testing
  (:require [cravendb.storage :as storage]
            [me.raynes.fs :as fs]))

(defn clear-test-data []
  (fs/delete-dir "testdir"))

(defn with-db [testfn]
  (clear-test-data)
  (with-open [db (storage/create-storage "testdir")]
    (testfn db)))

(defn inside-tx [testfn]
  (with-db 
    (fn [db]
      (with-open [tx (.ensure-transaction db)]
        (testfn tx)))))
