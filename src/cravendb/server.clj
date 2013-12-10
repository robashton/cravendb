(ns cravendb.server
  (:require 
            [org.httpkit.server :refer [run-server with-channel send! on-close]] 
            [compojure.handler :as handler]
            [cravendb.embedded :as embedded]
            [clojure.tools.logging :refer (info error debug)]
            [cravendb.http :as http]
            ))

(defn -main []
  (with-open [instance (embedded/create :path "testdb")]
    (run-server 
      (create-http-server instance) 
      { :port (Integer/parseInt (or (System/getenv "PORT") "8080")) :join? true}) 
    (debug "Shutting down mofo")))
