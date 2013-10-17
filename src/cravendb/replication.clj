(ns cravendb.replication
  (:require [cravendb.documents :as docs]
            [cravendb.core :refer [zero-etag etag-to-integer integer-to-etag]]
            [cravendb.storage :as s]
            [cravendb.client :as c]))

(defn replicate-into [tx items] 
  (reduce 
    (fn [{:keys [tx total last-etag] :as state} 
         {:keys [id doc metadata]}]
      (if doc
        (assoc state
          :tx (docs/store-document tx id doc (:etag metadata))
          :last-etag (:etag metadata)
          :total (inc total)) 
        (assoc state
          :tx (docs/delete-document tx id (:etag metadata))
          :last-etag (:etag metadata)
          :total (inc total)))) 
    { :tx tx :total 0 :last-etag (zero-etag) }
    items))

(defn store-last-etag [tx url etag]
  (s/store tx (str "replication-last-etag-" url) (etag-to-integer etag)))

(defn store-last-total [tx url total]
  (s/store tx (str "replication-total-documents-" url) total))

(defn last-replicated-etag [storage source-url]
  (integer-to-etag
    (s/get-integer storage (str "replication-last-etag-" source-url))))

(defn replication-total [storage source-url]
  (s/get-integer storage (str "replication-total-documents-" source-url)))

(defn replication-status 
  [storage source-url]
    {
     :last-etag (last-replicated-etag storage source-url)
     :total (replication-total storage source-url) })

(defn replicate-from [storage source-url items]
  (with-open [tx (s/ensure-transaction storage)] 
    (let [{:keys [tx last-etag total]} (replicate-into tx (take 100 items))] 
      (-> tx
        (store-last-etag source-url last-etag)
        (store-last-total source-url total)
        (s/commit!))))
    (drop 100 items))

(defn empty-replication-queue [storage-destination source-url etag]
  (loop [items (c/stream-seq source-url etag)]
    (if (not (empty? items))
      (recur (replicate-from storage-destination source-url items)))))

(defn replication-loop [storage source-url]
  (loop []
    (empty-replication-queue 
      storage     
      source-url
      (last-replicated-etag storage source-url))
    (Thread/sleep 50)
    (recur)))

(defn start [handle]
  (assoc handle
    :future (future (replication-loop 
                      (get-in handle [:instance :storage]) 
                      (:source-url handle)))))

(defn stop [handle]
  (if-let [f (:future handle)]
    (future-cancel f)))

(defrecord ReplicationHandle [instance source-url]
  java.io.Closeable
  (close [this]
    (stop this)))

(defn create [instance source-url]
  (ReplicationHandle. instance source-url))
