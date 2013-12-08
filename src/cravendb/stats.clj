(ns cravendb.stats
    (:require [clojure.core.async :refer [<! >! <!! put! chan go close! timeout mult tap ]]
              [clojure.tools.logging :refer [info error debug]]  
              [clj-time.core :as dt]))


(defn safe-inc [v a]
  (if v (+ v a) a))

(defn seconds-since-last-collect [stats]
  (dt/in-seconds (dt/interval (:last-collect stats) (dt/now))))

(defn snapshot [stats]
  (for [[k v] (:rolling stats)]
    {
     :per-second (float (/ v (seconds-since-last-collect stats)))
     :stat k 
  }))

(defn collect [stats out]
  (if (>= (seconds-since-last-collect stats) 1)
    (do
      (info "collecting stats")
      (go (put! out (snapshot stats)))
      (-> stats
        (dissoc :rolling)
        (assoc :last-collect (dt/now))))
    stats))

(defn go-coordinate [in out]
  (go
    (loop [stats { :last-collect (dt/now)}]
    (if-let [cmd (<! in)] (recur (collect (cmd stats) out))))))

(defn command [{:keys [commands]} cmd]
  (go (>! commands cmd)))

(defn append 
  ([counter ev] (append counter ev 1))
  ([counter ev am] 
   (command counter
    (fn [stats]
    (update-in stats [:rolling ev] safe-inc am)))))

(defn consume [counter in]
  (go
    (loop []
      (if-let [ev (<! in)]
        (do
          (if (coll? ev) 
            (apply append counter ev)
            (append counter ev))
          (recur))))))

(defn start [] 
  (let [cmd (chan)
        in (chan)
        events (mult in)] {
   :commands cmd
   :events events
   :loop (go-coordinate cmd in) }))

