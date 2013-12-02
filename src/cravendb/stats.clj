(ns cravendb.stats
    (:require [clojure.core.async :refer [<! >! <!! put! chan go close! timeout ]]
              [clojure.tools.logging :refer [info error debug]]  
              [clj-time.core :as dt]))

(defn safe-inc [v]
 (if v (inc v) 1))

(defn add-event [clump ev]
  (-> clump
    (update-in [ev :total] safe-inc)
    (update-in [ev :running] safe-inc)))

(defn seconds-since-last-collect [clump]
  (dt/in-seconds (dt/interval (:last-aggregate clump) (dt/now) )))

(defn snapshot [clump]
  (let [divider (seconds-since-last-collect clump)] 
    (into {}
      (for [[k v] clump]
        [k (:running (/ v divider))]))))

(defn reset [clump]
 (-> (reduce (fn [s [k v]] 
          (assoc-in s [k :running] 0)) 
        clump clump)
   (assoc :last-aggregate (dt/now))))

(defn aggregate [clump]
  (if (>= (seconds-since-last-collect clump) 1) 
    (-> clump
      (assoc :snapshot (snapshot clump))
      reset)
    clump))

(defn append-ev [clump ev]
  (-> 
    (add-event ev)
    (aggregate)))

(defn go-stats-loop [in]
  (go
    (loop [clump { :last-aggregate (dt/now)}]
      (if-let [ev (<! in)]
        (recur (append-ev clump ev))))))

(defn notify [ch ev]
  (go (put! ch ev)))

(defn total [k]

  )

(defn per-second [k]
  
  )

(defn create []
  (let [commands (chan)]
    (go-stats-loop commands)
    commands))
