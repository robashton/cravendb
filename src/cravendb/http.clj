(ns cravendb.http
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [clojure.edn :as edn]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [liberator.core :refer [resource]]
            [liberator.dev :refer [wrap-trace]]
            [cravendb.database :as db])

  (:use compojure.core
        [clojure.tools.logging :only (info error debug)]))

(defn read-body [ctx] (edn/read-string (slurp (get-in ctx [:request :body]))))

(def accepted-types ["application/edn" "text/plain" "text/html"])

(defn handle-response [ctx data]
  (case (get-in ctx [:representation :media-type])
    "text/plain" (pr-str data)
    "application/edn" (pr-str data)
    "text/html" (str "<p>" (pr-str data) "</p>"))) 

(defn craven-resource [])

(defn create-db-routes [instance]
  (routes
    (ANY "/document/:id" [id] 
      (resource
        :allowed-methods [:put :get :delete]
        :available-media-types accepted-types
        :put! (fn [ctx] (db/put-document instance id (read-body ctx))) 
        :delete! (fn [_] (db/delete-document instance id)) 
        :handle-ok (fn [_] (handle-response _ (db/load-document instance id)))))

    (ANY "/index/:id" [id]
      (resource
        :allowed-methods [:put :get :delete]
        :available-media-types accepted-types
        :put! (fn [ctx] (db/put-index instance (merge { :id id } (read-body ctx))))
        :delete! (fn [_] (db/delete-index instance id)) 
        :handle-ok (fn [_] (handle-response _ (db/load-index instance id)))))

    (ANY "/query/:index/:query" [index query]
       (resource
        :available-media-types accepted-types
        :handle-ok (fn [ctx] 
                     (handle-response 
                      ctx 
                      (db/query instance (get-in ctx [:request :params]))))))

    (ANY "/bulk" []
      (resource
        :allowed-methods [:post]
        :available-media-types ["application/edn"]
        :post! (fn [ctx] (pr-str (db/bulk instance (read-body ctx))))
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

