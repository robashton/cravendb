(ns cravendb.tasks
  (:refer-clojure :exclude [peek] )
  (:require [cravendb.storage :as s]))  

(def task-prefix "tasks-")

(defn queue [tx handle data] 
  (s/store tx (str task-prefix (s/next-synctag tx))
           (pr-str { :handle handle :data data})))

(defn delete [tx id]
  (s/delete tx id))

(defn peek [tx]
  (first 
     (map #(update-in %1 [:v] edn/read-string) 
          (let [iter (s/get-iterator tx)]
           (s/as-seq 
             (s/seek iter task-prefix))))))

(defn handle-task [id task handlers]
  (if-let [handler (handlers (:handle task))]
    (handler task)))

(defn pump [db handlers]
 (with-open [tx (s/ensure-transaction db)]
    (if-let [{:keys [k v]} (peek tx)]
      (do (handle-task k v handlers)
        (-> tx
          (delete k)
          (s/commit!))))))

