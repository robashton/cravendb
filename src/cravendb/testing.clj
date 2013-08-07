(ns cravendb.testing
  (:require [me.raynes.fs :as fs]
            [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.documents :as docs]
            [cravendb.core :as db]
            [cravendb.server :as server]))

(defn clear-test-data []
  (fs/delete-dir "testdir"))

(defn open-test-db []
  (docs/db "testdir"))

(defn with-db [testfn]
  (clear-test-data)
  (let [db (open-test-db)]
    (try 
      (testfn db)
      (finally
        (.close db) 
        (clear-test-data)))))

(defn with-test-server [testfn]
  (clear-test-data)
  (db/open "testdir")
  (let [server (run-jetty server/app { :port 9000 :join? false })]
    (try
      (testfn)
      (finally
        (.stop server)
        (db/close)
        (clear-test-data)))))
