(ns cravendb.server
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.core :as db]
            [compojure.route :as route]
            [compojure.handler :as handler])
  (:use compojure.core
        [clojure.tools.logging :only (info error)]))

(defroutes app-routes
  (PUT "/doc/:id" { params :params body :body }
      (let [id (params :id) body (slurp body)]
        (info "putting a document in with id " id " and body " body)
        (.-put (db/instance) id body)))
  (GET "/doc/:id" [id] 
    (info "getting a document with id " id)
    (or (.-get (db/instance) id) { :status 404 }))
  (DELETE "/doc/:id" [id])
  (route/not-found "<h1>Page not found</h1>"))

(def app
  (handler/api app-routes))

(defn -main []
  (db/open "testdb")
  (run-jetty app {:port (Integer/parseInt (System/getenv "PORT")) :join? true})
  (db/close))


