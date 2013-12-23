(ns cravendb.push
  (:require [clojure.core.async :refer [go <!]]
            [clojure.tools.logging :refer [info error debug]]
            [org.httpkit.server :refer [with-channel send! on-close]]))

(defn push-to [clients data]
  (doseq [channel clients]
    (send! channel {
                    :status 200
                    :headers {"Content-Type" "application/edn"}
                    :body (pr-str data)})))

(defn start 
  ([in] (start (fn [req] nil) in))
  ([immediate in]
    (let [hub (atom {})]
      (go (loop []
        (if-let [data (<! in)] 
          (do
            (info "Pushing data to clients")
            (push-to (keys @hub) data)
            (recur)))))
    (fn [request] 
      (if-let [immediate-response (immediate request)] immediate-response
        (with-channel request channel
          (swap! hub assoc channel request)
          (on-close channel (fn [status] (swap! hub dissoc channel)))))))))
