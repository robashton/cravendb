(ns cravendb.storage
  (:use [clojure.tools.logging :only (info debug error)])
  (:require [clojure.core.incubator :refer [dissoc-in]]
            [cravendb.memorystorage :as inmemory]
            [cravendb.levelstorage :as level]
            [cravendb.core :refer [zero-synctag integer-to-synctag synctag-to-integer]]))

(def last-synctag-key "__last-synctag")

(defn store [ops id data]
  (assoc-in ops [:cache id] data)) 

(defn delete [ops id]
  (assoc-in ops [:cache id] :deleted))

(defn get-obj [ops id]
  (let [cached (get-in ops [:cache id])]
    (if (= cached :deleted) nil
      (or cached (.from-db ops id)))))

(defn last-synctag-in
  [storage]
  (or (get-obj storage last-synctag-key) (zero-synctag)) )

(defn bootstrap-storage
  [storage]
  (assoc storage
    :last-synctag (atom (synctag-to-integer (last-synctag-in storage)))))

(defn next-synctag [{:keys [last-synctag]}]
  (integer-to-synctag (swap! last-synctag inc)))

(defn create-storage [dir]
  (bootstrap-storage (level/create dir)))

(defn get-iterator [storage]
  (.open-iterator storage))

(defn seek [iter value]
  (.seek! iter value))

(defn commit! [tx]
  (.commit! (store tx last-synctag-key 
              (integer-to-synctag @(:last-synctag tx)))))

(defn as-seq [iter]
  (.as-seq iter))

(defn ensure-transaction [storage]
  (assoc (.ensure-transaction storage)
         :last-synctag (:last-synctag storage)))

(defn create-in-memory-storage []
  (bootstrap-storage (inmemory/create)))
