(ns cravendb.tasks
  (:refer-clojure :exclude [peek] )
  (:require [cravendb.storage :as s]
            [clojure.edn :as edn]))  

(def task-prefix "tasks-")

(defn queue [tx queue handle data] 
  (s/store tx (str task-prefix queue (s/next-synctag tx))
           (pr-str { :handle handle :data data})))

(defn delete [tx id]
  (s/delete tx id))

(defn peek [tx queue]
  (first 
     (map #(update-in %1 [:v] edn/read-string) 
          (let [iter (s/get-iterator tx)]
           (s/as-seq 
             (s/seek iter (str task-prefix queue)))))))

(defn handle-task [db id task handlers]
  (if-let [handler (handlers (:handle task))]
    (handler db task)))

(defn pump [db queue handlers]
 (with-open [tx (s/ensure-transaction db)]
    (if-let [{:keys [k v]} (peek tx)]
      (do (handle-task db k v handlers)
        (-> tx
          (delete k)
          (s/commit!))))))

