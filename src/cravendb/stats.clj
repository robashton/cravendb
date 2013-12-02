(ns cravendb.stats
    (:require [clojure.core.async :refer [<! >! <!! put! chan go close! timeout ]]
              [clojure.tools.logging :refer [info error debug]]  
              [clj-time.core :as dt]))

(defn safe-inc [v]
 (if v (inc v) 1))

(defn add-event [clump ev]
  (-> clump
    (update-in [:events ev :total] safe-inc)
    (update-in [:events ev :running] safe-inc)))

(defn seconds-since-last-collect [clump]
  (dt/in-seconds (dt/interval (:last-aggregate clump) (dt/now) )))

(defn snapshot [{:keys [events] :as clump}]
  (let [divider (seconds-since-last-collect clump)] 
    (into {}
      (for [[k v] events]
        [k (float (/ (:running v) divider))]))))

(defn reset [{:keys [events] :as clump}]
 (-> (assoc clump :events 
            (reduce (fn [s [k v]] (assoc-in s [k :running] 0)) 
        events events))
     (assoc :last-aggregate (dt/now))))

(defn aggregate [clump]
  (if (>= (seconds-since-last-collect clump) 1) 
    (reset (assoc clump :snapshot (snapshot clump)))
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

(defn create []
  (let [commands (chan)]
    (go-stats-loop commands)
    commands))
