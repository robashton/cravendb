(ns cravendb.server
  (:require [ring.adapter.jetty :refer [run-jetty]]
            [clojure.tools.nrepl.server :as nrepl]
            [cravendb.core :as core]
            [ring.util.response :refer [response]]))

(defn handler [req]
  (response "Hello world"))

(defn -main []
  (run-jetty #'handler {:port (Integer/parseInt (System/getenv "PORT"))}))
