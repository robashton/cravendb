(ns cravendb.replication
  (:require [cravendb.documents :as docs]
            [cravendb.core :refer [zero-synctag synctag-to-integer integer-to-synctag]]
            [cravendb.storage :as s]
            [cravendb.vclock :as v]
            [cravendb.client :as c]))

(defn conflict-status 
  [current candidate]
  (cond
    (nil? current) :write
    (v/same? candidate current) :skip
    (v/descends? candidate current) :write
    (v/descends? current candidate) :skip
    :else :conflict))

(defn status-for
  [tx {:keys [id metadata]}]
  (conflict-status
    (get (docs/load-document-metadata tx id) :history)
    (get metadata :history)))

(defn action-for
  [item]
  (if (:doc item) 
    :store
    :delete))

(defn action-into-tx 
  [tx {:keys [id doc metadata] :as item}]
  (case [(status-for tx item) (action-for item)]
    [:skip :delete] tx
    [:skip :store] tx
    [:write :store] (docs/store-document tx id doc metadata)
    [:write :delete] (docs/delete-document tx id metadata)
    [:conflict :store] (docs/store-conflict tx id doc metadata)
    [:conflict :delete] (docs/store-conflict tx id :deleted metadata)))

(defn replicate-into [tx items] 
  (reduce 
    (fn [{:keys [tx total last-synctag] :as state} 
         {:keys [id doc metadata] :as item}]
      (assoc state
        :tx (action-into-tx tx item)
        :last-synctag (:synctag metadata)
        :total (inc total)))
    { :tx tx :total 0 :last-synctag (zero-synctag) }
    items))

(defn store-last-synctag [tx url synctag]
  (s/store tx (str "replication-last-synctag-" url) (synctag-to-integer synctag)))

(defn store-last-total [tx url total]
  (s/store tx (str "replication-total-documents-" url) total))

(defn last-replicated-synctag [storage source-url]
  (integer-to-synctag
    (s/get-integer storage (str "replication-last-synctag-" source-url))))

(defn replication-total [storage source-url]
  (s/get-integer storage (str "replication-total-documents-" source-url)))

(defn replication-status 
  [storage source-url]
    {
     :last-synctag (last-replicated-synctag storage source-url)
     :total (replication-total storage source-url) })

(defn replicate-from [storage source-url items]
  (with-open [tx (s/ensure-transaction storage)] 
    (let [{:keys [tx last-synctag total]} (replicate-into tx (take 100 items))] 
      (-> tx
        (store-last-synctag source-url last-synctag)
        (store-last-total source-url total)
        (s/commit!))))
    (drop 100 items))

(defn empty-replication-queue [storage-destination source-url synctag]
  (loop [items (c/stream-seq source-url synctag)]
    (if (not (empty? items))
      (recur (replicate-from storage-destination source-url items)))))

(defn replication-loop [storage source-url]
  (loop []
    (empty-replication-queue 
      storage     
      source-url
      (last-replicated-synctag storage source-url))
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

(defn wait-for  
  ([handle synctag] (wait-for handle synctag 1000))
  ([handle synctag timeout]
  (if (and 
        (not= synctag 
         (last-replicated-synctag (get-in handle [:instance :storage])
                        (:source-url handle)))
        (> timeout 0))
    (do
      (Thread/sleep 100)
      (wait-for handle synctag (- timeout 100))))))
