(ns cravendb.http
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [liberator.core :refer [resource]]
            [liberator.dev :refer [wrap-trace]]
            [cravendb.database :as db])

  (:use compojure.core
        [clojure.tools.logging :only (info error debug)]))

(defn create-db-routes [instance]
  (routes
    (ANY "/document/:id" [] 
      (resource
        :allowed-methods [:put :get :delete]
        :available-media-types ["application/clojure" "text/plain"]
        :put! (fn [ctx]
                (let [body (slurp (get-in ctx [:request :body]))
                      id (get-in ctx [:request :params :id])] 
                  (debug id body)
                  (db/put-document instance id body)))
        :delete! (partial db/delete-document instance) 
        :handle-ok (partial db/load-document instance)))


    (GET "/query/:index/:query" { params :params  }
      (db/query instance params))

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
        { :stats 404}))))

(defn create-http-server [instance]
  (info "Setting up the bomb")
  (let [db-routes (create-db-routes instance)]
    (-> (handler/api db-routes)
      (wrap-trace :header :ui))))

(defn -main []
  (with-open [instance (db/create "testdb")]
    (run-jetty 
      (create-http-server instance) 
      { :port (Integer/parseInt (or (System/getenv "PORT") "8080")) :join? true}) 
    (debug "Shutting down")))

