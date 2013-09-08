(ns cravendb.http
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [cravendb.storage :as storage]
            [cravendb.documents :as docs])
  (:use compojure.core
        [clojure.tools.logging :only (info error)]))


(defn create-http-server [db]
  (defroutes app-routes

    (PUT "/doc/:id" { params :params body :body }
      (let [id (params :id) body (slurp body)]
        (info "putting a document in with id " id " and body " body)
        (with-open [tx (.ensure-transaction db)]
          (.commit (docs/store-document tx id body)))))

    (GET "/doc/:id" [id] 
      (info "getting a document with id " id)
         (with-open [tx (.ensure-transaction db)]
          (or (docs/load-document tx id) { :status 404 })))

    (DELETE "/doc/:id" [id]
      (info "deleting a document with id " id)
        (with-open [tx (.ensure-transaction db)]
          (.commit (docs/delete-document tx id))))

    (route/not-found "ZOMG NO, THIS IS NOT A VALID URL"))

  (handler/api app-routes))

(defn -main []
  (with-open [db (storage/create-storage "testdb")]
   (run-jetty (create-http-server db) {:port (Integer/parseInt (System/getenv "PORT")) :join? true})))
