(ns cravendb.http
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [cravendb.storage :as storage]
            [cravendb.indexing :as indexing] 
            [cravendb.query :as query] 
            [cravendb.indexstore :as indexes] 
            [cravendb.indexengine :as indexengine] 
            [cravendb.documents :as docs])

  (:use compojure.core
        [clojure.tools.logging :only (info error debug)]))

(defn interpret-bulk-operation [tx op]
  (case (:operation op)
    :docs-delete (docs/delete-document tx (:id op))
    :docs-put (docs/store-document tx (:id op) (pr-str (:document op)))) ;;  TODO: NO
  )

(defn create-http-server [db index-engine]
  (info "Setting up the bomb")

  (defroutes app-routes

    (GET "/query/:index/:query" { params :params  }
      (let [q (params :query)
            w (params :wait)]
        (debug "Querying for " q  "waiting: " w)
        (query/execute db index-engine params)))

    (PUT "/doc/:id" { params :params body :body }
      (let [id (params :id) body (slurp body)]
        (debug "putting a document in with id " id " and body " body)
        (with-open [tx (.ensure-transaction db)]
          (.commit! (docs/store-document tx id body)))))

    (GET "/doc/:id" [id] 
      (debug "getting a document with id " id)
         (with-open [tx (.ensure-transaction db)]
           (or (docs/load-document tx id) { :status 404 })))

    (DELETE "/doc/:id" [id]
      (debug "deleting a document with id " id)
        (with-open [tx (.ensure-transaction db)]
          (.commit! (docs/delete-document tx id))))

    (POST "/bulk" { body-in :body }
      (let [body ((comp read-string slurp) body-in)]
        (debug "Bulk operation: " body)
        (with-open [tx (.ensure-transaction db)]
          (.commit! 
            (reduce
              interpret-bulk-operation
              tx
              body))))
          "OK")

    (PUT "/index/:id" { params :params body :body }
      (let [id (params :id) body ((comp read-string slurp) body)]
        (debug "putting an in with id " id " and body " body)
        (with-open [tx (.ensure-transaction db)]
          (.commit! 
            (indexes/put-index 
              tx {
                  :id id
                  :map (body :map)
                 })))))

    (GET "/index/:id" [id] 
      (debug "getting an index with id " id)
         (with-open [tx (.ensure-transaction db)]
           (let [index (indexes/load-index tx id)]
             (if index
               (pr-str index)
               { :status 404 }))))

    (route/not-found "ZOMG NO, THIS IS NOT A VALID URL"))

  (handler/api app-routes))

(defn -main []
  (with-open [db (storage/create-storage "testdb")
              engine (indexengine/create-engine db)]
    (try
      (.start engine)
      (run-jetty 
        (create-http-server db engine) 
        { :port (Integer/parseInt (or (System/getenv "PORT") "8080")) :join? true})   
      (finally
        (.stop engine)))
    
    (debug "Shutting down")))

