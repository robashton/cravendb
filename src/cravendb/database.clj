(ns cravendb.database
  (:require [cravendb.storage :as s]
            [cravendb.indexing :as indexing] 
            [cravendb.query :as query] 
            [cravendb.indexstore :as indexes] 
            [cravendb.indexengine :as indexengine] 
            [cravendb.documents :as docs]
            [cravendb.vclock :as vclock]
            [clojure.tools.logging :refer [info error debug]]))

(defrecord Database [storage index-engine]
  java.io.Closeable
  (close [this] 
    (indexengine/stop index-engine)
    (.close index-engine)
    (.close storage)))

(defn create
  [path & opts]
  (let [storage (s/create-storage path)
        index-engine (indexengine/create-engine storage)]
    (indexengine/start index-engine)
    (merge (Database. storage index-engine)
           { :server-id "root"
             :base-vclock (vclock/new)
             :tx-count (atom 0) }
            (apply hash-map opts))))

(defn interpret-bulk-operation [{:keys [tx instance] :as state} op]
  (assoc state :tx 
    (case (:operation op)
      :docs-delete (docs/delete-document tx (:id op))
      :docs-put (docs/store-document tx (:id op) (:document op) 
                                   {:synctag (s/next-synctag tx)}))))


(def default-query { :index "default"
                     :wait-duration 5
                     :wait false
                     :query "*"
                     :sort-order :asc
                     :sort-by nil
                     :offset 0
                     :amount 1000 })
(defn query
  [{:keys [storage index-engine]} params]
  (debug "Querying for " params)
  (query/execute storage index-engine (merge default-query params)))

(defn clear-conflicts [{:keys [storage]} id]
  (with-open [tx (s/ensure-transaction storage)] 
    (s/commit! (docs/without-conflicts tx id))))

(defn in-tx 
  [{:keys [storage last-synctag tx-count server-id base-vclock] :as instance} f]
  (with-open [tx (s/ensure-transaction storage)] 
    (-> tx
      (assoc :e-id (str server-id (swap! tx-count inc))
             :base-vclock base-vclock
             :server-id server-id)
      (f)
      (s/commit!))
    (swap! tx-count dec)))

(defn load-document-metadata
  [{:keys [storage]} id]
  (debug "getting document metadata id " id)
  (docs/load-document-metadata storage id))

(defn checked-history [tx supplied-history existing-history]
  (let [last-version (or  supplied-history existing-history (:base-vclock tx))
        next-version (vclock/next (:e-id tx) last-version)]
    {:is-conflict (not (vclock/descends? (or supplied-history next-version) (or existing-history last-version)))
     :history next-version}))

(defn load-document-metadata
  [{:keys [storage]} id]
  (debug "getting document metadata id " id)
  (docs/load-document-metadata storage id))

(defn check-document-write [tx id metadata success conflict]
  (let [history-result 
        (checked-history tx
          (:history metadata) (:history (or (docs/load-document-metadata tx id) {})))]
         ((if (:is-conflict history-result) conflict success)
            (assoc metadata :history (:history history-result)
                            :primary-server (:server-id tx)
                            :synctag (s/next-synctag tx)))))

(defn put-document 
  ([instance id document] (put-document instance id document {}))
  ([{:keys [last-synctag] :as instance} id document metadata]
  (debug "putting a document:" id document metadata)
   (in-tx instance 
     (fn [tx]
       (check-document-write 
         tx id metadata
         #(docs/store-document tx id document %1)
         #(docs/store-conflict tx id document %1))))))

(defn delete-document 
  ([instance id] (delete-document instance id nil))
  ([instance id metadata]
  (debug "deleting a document with id " id)
   (in-tx instance 
     (fn [tx]
       (check-document-write
         tx id metadata
         #(docs/delete-document tx id %1)
         #(docs/store-conflict tx id :deleted %1))))))

(defn load-document 
  [{:keys [storage]} id]
  (debug "getting a document with id " id)
  (docs/load-document storage id))

(defn bulk 
  [instance operations]
  (debug "Bulk operation: ")
  (in-tx instance 
    (fn [tx] (:tx 
    (reduce
      interpret-bulk-operation
      {:tx tx :instance instance}
      operations)))))

(defn put-index 
  [instance index]
  (debug "putting an in index:" index)
  (in-tx instance 
    (fn [tx] 
      (indexes/put-index tx index {:synctag (s/next-synctag tx)}))))

(defn load-index-metadata
  [{:keys [storage]} id]
  (debug "getting index metadata id " id)
  {:synctag (indexes/synctag-for-index storage id)})

(defn delete-index 
  [instance id]
  (debug "deleting an index" id)
  (in-tx instance 
    (fn [tx] (indexes/delete-index tx id {:synctag (s/next-synctag tx)}))))

(defn load-index 
  [{:keys [storage]} id]
  (debug "getting an index with id " id)
  (with-open [tx (s/ensure-transaction storage)]
    (indexes/load-index tx id)))
