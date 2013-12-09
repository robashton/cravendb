(ns cravendb.transaction
   (:refer-clojure :exclude [load] )
   (:require [cravendb.client :as client]
             [cravendb.database :as db]))

(defn mode 
  "Determines whether a transaction is operating on an embedded or remote endpoint
  Returns either :remote or :embedded"
  [tx & _] (if (:href tx) :remote :embedded))

(defmulti from-storage 
  "Retrieves a document by id directly from the underlying storage without checking the transaction cache"
  mode)
(defmethod from-storage :remote 
  [{:keys [href]} id]
  (client/get-document href id))

(defmethod from-storage :embedded 
  [{:keys [instance]} id]
  (db/load-document instance id))

(defn store
  "Stores any clojure object as a document in the transaction with the specified id"
  [tx id data]
  (assoc-in tx [:cache id] data))

(defn delete [tx id]
  "Deletes a document by id in the current transaction"
  (assoc-in tx [:cache id] :deleted))

(defn load
  "Loads a document from the current transaction by id
  will honour any changes made in previous operations"
  [tx id]
  (let [cached (get-in tx [:cache id])]
    (if (= cached :deleted) nil
      (or cached (from-storage tx id)))))

(defn package 
  "Packages up a transaction's cache into a bulk operation which can be submitted either over HTTP or directly to a database instance"
  [cache]
  (into () 
    (for [[k v] cache]
      (if (= v :deleted)
        { :operation :docs-delete :id k }
        { :operation :docs-put :id k :document v }))))

(defmulti commit! 
  "flushes a transaction as a bulk operation to the underlying storage - this is atomic"
  mode)
(defmethod commit! :remote 
  [{:keys [href] :as tx}]
  (client/bulk-operation href (package (:cache tx))))
(defmethod commit! :embedded 
  [{:keys [instance] :as tx}]
  (db/bulk instance (package (:cache tx))))

(defn open 
  "Starts a transaction ready for working with"
  [{:keys [href] :as instance}] 
  { :href href :instance instance})
p
