(ns cravendb.push
  (:require [clojure.core.async :refer [go <!]]
            [clojure.tools.logging :refer [info error debug]]
            [org.httpkit.server :refer [with-channel send! on-close]]))

(defn push-to [clients #spy/p data]
  (doseq [channel clients]
    (send! channel {
                    :status 200
                    :headers {"Content-Type" "application/edn"}
                    :body data})))

(defn start 
  [in]
  (let [hub (atom {})]
    (go (loop []
      (if-let [data (<! in)] 
        (do
          (info "Pushing stats to clients")
          (push-to (keys @hub) data)
          (recur)))))
    (fn [request] 
        (with-channel request channel
          (swap! hub assoc channel request)
          (on-close channel (fn [status] (swap! hub dissoc channel)))))))
