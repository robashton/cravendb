(ns cravendb.push
  (:require [clojure.core.async :refer [go <!]]
            [clojure.tools.logging :refer [info error debug]]
            [org.httpkit.server :refer [with-channel send! on-close]]))

(defn start 
  [in]
  (let [hub (atom {})]
    (go (loop []
      (if-let [data (<! in)] 
        (do
          (info "Pushing stats to clients")
          (doseq [channel (keys @hub)]
          (send! channel {
                   :status 200
                   :headers {"Content-Type" "application/json; charset=utf-8"}
                   :body data}))
          (recur)))))
    (fn [request] 
        (with-channel request channel
          (swap! hub assoc channel request)
          (on-close channel (fn [status] (swap! hub dissoc channel)))))))
