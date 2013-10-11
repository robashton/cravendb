(ns cravendb.http
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [cravendb.database :as db])

  (:use compojure.core
        [clojure.tools.logging :only (info error debug)]))

(defn create-db-routes [instance]
  (routes
    (GET "/query/:index/:query" { params :params  }
      (db/query instance params))

    (PUT "/doc/:id" { params :params body :body }
      (let [id (params :id) body (slurp body)]
        (db/put-document instance id body)))

    (GET "/doc/:id" [id] 
      (or (db/load-document instance id) { :status 404 }))

    (DELETE "/doc/:id" [id]
      (debug "deleting a document with id " id)
      (db/delete-document instance id))

    (POST "/bulk" { body-in :body }
      (let [body ((comp read-string slurp) body-in)]
        (db/bulk instance body)) 
      "OK")

    (PUT "/index/:id" { params :params body :body }
      (let [id (params :id) body ((comp read-string slurp) body)]
        (db/put-index instance (merge body { :id id })))
         "OK")

    (DELETE "/index/:id" [id]
      (db/delete-index instance id))

    (GET "/index/:id" [id] 
      (if-let [index (db/load-index instance id)]
        (pr-str index)
        { :stats 404}))

    (route/not-found "ZOMG NO, THIS IS NOT A VALID URL")))

(defn create-http-server [instance]
  (info "Setting up the bomb")
  (let [db-routes (create-db-routes instance)]
    (handler/api db-routes)))

(defn -main []
  (with-open [instance (db/create "testdb")]
    (run-jetty 
      (create-http-server instance) 
      { :port (Integer/parseInt (or (System/getenv "PORT") "8080")) :join? true}) 
    (debug "Shutting down")))

