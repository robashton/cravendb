(ns cravendb.http
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [liberator.core :refer [resource]]
            [liberator.dev :refer [wrap-trace]]
            [cravendb.database :as db])

  (:use compojure.core
        [clojure.tools.logging :only (info error debug)]))

(defn read-body [ctx] (read-string (slurp (get-in ctx [:request :body]))))

(defn create-db-routes [instance]
  (routes
    (ANY "/document/:id" [id] 
      (resource
        :allowed-methods [:put :get :delete]
        :available-media-types ["application/clojure"]
        :put! (fn [ctx] (db/put-document instance id (read-body ctx))) 
        :delete! (fn [_] (db/delete-document instance id)) 
        :handle-ok (fn [_] (db/load-document instance id))))

    (ANY "/index/:id" [id]
      (resource
        :allowed-methods [:put :get :delete]
        :available-media-types ["application/clojure"]
        :put! (fn [ctx] (db/put-index instance (merge { :id id } (read-body ctx))))
        :delete! (fn [_] (db/delete-index instance id)) 
        :handle-ok (fn [_] (db/load-index instance id))))

    (ANY "/query/:index/:query" [index query]
       (resource
        :available-media-types ["application/clojure"]
        :handle-ok (fn [ctx] (db/query instance (get-in ctx [:request :params])))))

    (ANY "/bulk" 
      (resource
        :allowed-methods [:post]
        :available-media-types ["application/clojure"]
        :post! (fn [ctx] (db/bulk instance (read-body ctx)))
        :handle-ok "OK"))))

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

