(ns cravendb.push)

(start 
  [in]
  (let [hub (atom {})]
    (go (loop []
      (if-let [data (<! in)] 
        (do
          (doseq [channel (keys @hub)]
          (send! channel {
                   :status 200
                   :headers {"Content-Type" "application/json; charset=utf-8"}
                   :body data}))
          (recur)))))
    (fn [request] 
        (with-channel request channel
        (swap! hub assoc channel request)
        (on-close channel (fn [status]
        (swap! hub dissoc channel)))))))
