(ns cravendb.testing
  (:require [me.raynes.fs :as fs]
            [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.documents :as docs]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as s]
            [cravendb.http :as http]
            [cravendb.database :as database]))
      

(defn clear-test-data []
  (fs/delete-dir "testdir"))

(defn with-db [testfn]
  (clear-test-data)
  (with-open [db (s/create-storage "testdir")]
    (testfn db))
  (clear-test-data))


(defn with-full-setup [testfn]
  (clear-test-data)
  (with-open [instance (database/create "testdir")]
    (let [result (testfn instance)]
      (clear-test-data)      
        result)))

(defn inside-tx [testfn]
  (with-db 
    (fn [db]
      (with-open [tx (s/ensure-transaction db)]
        (testfn tx)))))

(defn with-test-server [testfn]
  (clear-test-data)
  (with-open [instance (database/create "testdir")]
    (try (let [server (run-jetty 
                   (http/create-http-server instance) 
                    { :port 9000 :join? false} )]
      (try
        (testfn)
        (finally
          (.stop server))))))
  (clear-test-data))

(defn start-server 
  ([] (start-server 8080))
  ([port]
  (fs/delete-dir (str "testdir" port))
  (let [instance (database/create (str "testdir" port))] 
    {
    :port port
    :url (str "http://localhost:" port)
    :instance instance
    :server (run-jetty (http/create-http-server instance)
                        {:port port :join? false}) })))

(defn stop-server [info]
  (.stop (:server info))
  (.close (:instance info))
  (fs/delete-dir (str "testdir" (:port info))))
