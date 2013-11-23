(ns cravendb.clienttransaction
   (:require [cravendb.client :as client]
             [cravendb.database :as db]))


(defn mode [tx & _] (if (:href tx) :remote :embedded))

(defn store-document [tx id data]
  (assoc-in tx [:cache id] data))

(defn delete-document [tx id]
  (assoc-in tx [:cache id] :deleted))

(defmulti from-storage mode)
(defmethod from-storage :remote [{:keys [href]} id]
  (client/get-document href id))
(defmethod from-storage :embedded [{:keys [instance]} id]
  (db/load-document instance id))

(defn load-document [tx id]
  (let [cached (get-in tx [:cache id])]
    (if (= cached :deleted) nil
      (or cached (from-storage tx id)))))

(defn package [cache]
  (into () 
    (for [[k v] cache]
      (if (= v :deleted)
        { :operation :docs-delete :id k }
        { :operation :docs-put :id k :document v }))))

(defmulti commit! mode)
(defmethod commit! :remote 
  [{:keys [href] :as tx}]
  (client/bulk-operation href (package (:cache tx))))
(defmethod commit! :embedded 
  [{:keys [instance] :as tx}]
  (db/bulk instance (package (:cache tx))))

(defn start [href] { :href href})
