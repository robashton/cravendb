(ns cravendb.server
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.core :as db]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [cravendb.documents :as docs])
  (:use compojure.core
        [clojure.tools.logging :only (info error)]))

(defroutes app-routes
  (PUT "/doc/:id" { params :params body :body }
      (let [id (params :id) body (slurp body)]
        (info "putting a document in with id " id " and body " body)
        (db/perform (fn [ops] (docs/store-document ops id body)))))
  (GET "/doc/:id" [id] 
    (info "getting a document with id " id)
    (or (db/perform (fn [ops] (docs/load-document ops id))) 
        { :status 404 }))
  (DELETE "/doc/:id" [id]
    (info "deleting a document with id " id)
    (db/perform (fn [ops] (docs/delete-document ops id))))
  (route/not-found "ZOMG NO, THIS IS NOT A VALID URL"))

(def app
  (handler/api app-routes))

(defn -main []
  (db/open "testdb")
  (run-jetty app {:port (Integer/parseInt (System/getenv "PORT")) :join? true})
  (db/close))


