(ns cravendb.routes
  (:require [clojure.core.async :refer [tap chan]]
            [compojure.route :as route]
            [liberator.core :refer [resource]]
            [liberator.dev :refer [wrap-trace]]
            [liberator.representation :refer [ring-response]]
            [cravendb.database :as db]
            [cravendb.stream :as stream]
            [cravendb.push :as push]
            [cravendb.core :refer [zero-synctag integer-to-synctag]]
            [clojure.tools.logging :refer (info error debug)])
            [cravendb.http :as http]
  (:use compojure.core))

(defn create [instance]
  (routes
    (ANY "/document/:id" [id] 
      (resource
        :allowed-methods [:put :get :delete :head]
        :exists? (fn [ctx] (resource-exists ctx #(db/load-document instance id) #(db/load-document-metadata instance id)))
        :available-media-types accepted-types
        :etag (fn [ctx] (etag-from-metadata ctx))
        :put! (fn [ctx] (db/put-document instance id (read-body ctx) (read-metadata ctx)))
        :delete! (fn [_] (db/delete-document instance id (read-metadata _)))
        :handle-ok (fn [_] (standard-response _ (::resource _) (::metadata _)))))

    (ANY "/index/:id" [id]
      (resource
        :allowed-methods [:put :get :delete :head]
        :exists? (fn [ctx] (resource-exists ctx #(db/load-index instance id) #(db/load-index-metadata instance id)))
        :available-media-types accepted-types
        :etag (fn [ctx] (etag-from-metadata ctx))
        :put! (fn [ctx] (db/put-index instance (merge { :id id } (read-body ctx))))
        :delete! (fn [_] (db/delete-index instance id)) 
        :handle-ok (fn [_] (standard-response _ (::resource _) (::metadata _) ) )))

    (ANY "/query/:index/:filter" [index filter]
       (resource
        :available-media-types accepted-types
        :handle-ok (fn [ctx] 
                     (standard-response 
                      ctx 
                      (db/query instance 
                                (merge { :filter filter :index index} 
                                       (query-string (get-in ctx [:request :params]) {
                                               :wait-duration #(Integer/parseInt %1)
                                               :wait boolean
                                               :sort-order symbol
                                               :sort-by str
                                               :offset #(Integer/parseInt %1)
                                               :amount #(Integer/parseInt %1) }))) {}))))

    (ANY "/conflict/:id" [id]
      (resource
        :allowed-methods [:delete]
        :available-media-types accepted-types
        :delete! (fn [_] (db/clear-conflicts instance id))))

    ;; ANOTHER UWAGA!!
    (ANY "/conflicts" []
       (resource
        :available-media-types accepted-types
        :handle-ok (fn [ctx] 
                     (standard-response ctx (db/conflicts instance) {}))))


    (ANY "/bulk" []
      (resource
        :allowed-methods [:post]
        :available-media-types ["application/edn"]
        :post! (fn [ctx] (pr-str (db/bulk instance (read-body ctx))))
        :handle-ok "OK"))

    (ANY "/stats" [] (push/start (tap (get-in instance [:counters :events]) (chan))))

    (ANY "/stream" []
      (resource
        :allowed-methods [:get :head]
        :exists? true
        :available-media-types accepted-types
        :handle-ok 
        (fn [ctx]
          (standard-response 
            ctx
            (stream/from-synctag 
              instance
              (or (get-in ctx [:request :params :synctag]) (zero-synctag))) 
            {}
            "last-synctag" (integer-to-synctag @(get-in instance [:storage :last-synctag])) ))))

    (route/files "/admin/" { :root "admin"} ))) 


