(ns cravendb.testing
  (:require [me.raynes.fs :as fs]
            [org.httpkit.server :refer [run-server]]
            [cravendb.documents :as docs]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as s]
            [cravendb.http :as http]
            [cravendb.database :as db]
            [cravendb.embedded :as embedded]
            [cravendb.remote :as remote]
            [speclj.core :as spec]))
      

(defn clear-test-data []
  (fs/delete-dir "testdir"))

(defn with-db [testfn]
  (clear-test-data)
  (with-open [db (s/create-storage "testdir")]
    (testfn db))
  (clear-test-data))

(defn testing-path
  ([& v]
  (if (get (System/getenv) "IN_MEMORY")
    nil
    (apply str "testdir" v))))

(defn with-full-setup [testfn]
  (clear-test-data)
  (with-open [instance (embedded/create :path (testing-path))]
    (testfn instance)))

(defn inside-tx [testfn]
  (with-db 
    (fn [db]
      (with-open [tx (s/ensure-transaction db)]
        (testfn tx)))))

(defn with-test-server [testfn]
  (clear-test-data)
  (with-open [instance (embedded/create :path (testing-path))]
    (try (let [server (run-server 
                   (http/create-http-server instance) 
                    { :port 9000 :join? false} )]
      (try
        (testfn)
        (finally
          (server))))))
  (clear-test-data))

(defn start-server 
  ([] (start-server 8080))
  ([port & opts]
  (fs/delete-dir (str "testdir" port))
  (let [instance (apply embedded/create :path (testing-path port) opts)] 
    {
    :port port
    :url (str "http://localhost:" port)
    :instance instance
    :server (run-server (http/create-http-server instance)
                        {:port port :join? false}) })))


(defmacro with-remote [& body]
  `(with-test-server 
     (fn [] ~@body)))

(defmacro with-remote-instance [& body]
  `(with-remote
     (let [~'instance (remote/create :href "http://localhost:9000")] ~@body)))

(defmacro multi [description & body]
  `(spec/describe "with the various storage mediums"
    (spec/describe "embedded in-memory"
      (spec/it ~description
        (with-open [~'instance (embedded/create)] ~@body)))
   (spec/describe "embedded on-disk"
    (spec/it ~description
      (fs/delete-dir "testdir") 
      (with-open [~'instance (embedded/create :path "testdir")] ~@body)
      (fs/delete-dir "testdir")))                
    (spec/describe "remote"
      (spec/it ~description
        (with-remote
          (with-open [~'instance (remote/create :href "http://localhost:9000")] ~@body))))))

(defn stop-server [info]
  ((:server info)) 
  (db/close (:instance info))
  (fs/delete-dir (str "testdir" (:port info))))
