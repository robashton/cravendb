(ns cravendb.testing
  (:require [me.raynes.fs :as fs]
            [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.documents :as docs]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as storage]
            [cravendb.http :as http]))

(defn clear-test-data []
  (fs/delete-dir "testdir"))

(defn with-db [testfn]
  (clear-test-data)
  (with-open [db (storage/create-storage "testdir")]
    (testfn db))
  (clear-test-data))

(defn inside-tx [testfn]
  (with-db 
    (fn [db]
      (with-open [tx (.ensure-transaction db)]
        (testfn tx)))))

(defn with-test-server [testfn]
  (clear-test-data)
  (with-open [db (indexengine/start (storage/create-storage "testdir"))
              loaded-indexes (indexengine/load-from db) ]
    (let [server (run-jetty (http/create-http-server db) { :port 9000 :join? false} )]
      (try
        (testfn)
        (finally
          (indexengine/teardown db)
          (.stop server)))))
  (clear-test-data))
