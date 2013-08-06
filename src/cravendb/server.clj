(ns cravendb.server
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.core :as db]
            [compojure.route :as route]
            [compojure.handler :as handler])
  (:use compojure.core))

(defroutes app-routes
  (PUT "/doc/:id" { id :id body :body } (slurp body)) 
  (GET "/doc/:id" [id] 
    (.-get (db/instance) id))
  (DELETE "/doc/:id" [id])

  (route/not-found "<h1>Page not found</h1>"))

(def app
  (handler/api app-routes))

(defn -main []
  (db/open)
  (run-jetty app {:port (Integer/parseInt (System/getenv "PORT")) :join? true})
  (db/close))


