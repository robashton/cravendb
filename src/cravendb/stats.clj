(ns cravendb.stats
    (:require [clojure.core.async :refer [<! >! <!! put! chan go close! ]]))

(defn safe-inc [v]
 (if v (inc v) 1))

(defn add-event [clump ev]
  (-> clump
    (update-in [ev :total] safe-inc)
    (update-in [ev :running] safe-inc)))

(defn snapshot [clump]
  (into {}
    (for [[k v] clump]
      [k (:running v)])))

(defn aggregate [clump]
  (-> clump
    (assoc :snapshot (snapshot clump))))


(-> {}
  (add-event :index)
  (add-event :index)
  (add-event :index)
  aggregate
  )

(update-in {} [:blah :total] #(if %1 inc 0))

(defn go-stats-loop [commands]
  (go
    (loop [clump {}]
      (if-let [{:keys [cmd data]} (<! commands)]
        (recur (case cmd
                 :notify (add-event clump data)
                 :tick (aggregate clump))))))) 

(defn notify [k]
  
  )

(defn total [k]
  
  )

(defn per-second [k]
  
  )

(defn create []

  )
