(ns cravendb.database
  (:require [cravendb.storage :as s]
            [cravendb.indexing :as indexing] 
            [cravendb.query :as query] 
            [cravendb.indexstore :as indexes] 
            [cravendb.indexengine :as indexengine] 
            [cravendb.documents :as docs]
            [cravendb.vclock :as vclock]
            [clojure.tools.logging :refer [info error debug]]
            [cravendb.core :refer [zero-synctag integer-to-synctag synctag-to-integer]]))


(defrecord Database [storage index-engine]
  java.io.Closeable
  (close [this] 
    (indexengine/stop index-engine)
    (.close index-engine)
    (.close storage)))

(defn create
  [path]
  (let [storage (s/create-in-memory-storage)
        index-engine (indexengine/create-engine storage)]
    (indexengine/start index-engine)
    (assoc (Database. storage index-engine)
           :last-synctag (atom (synctag-to-integer (docs/last-synctag-in storage)))
           :tx-count (atom 0)
           :base-vclock (vclock/new)
           :server-id "root"
           )))

(defn next-synctag [last-synctag]
    (integer-to-synctag (swap! last-synctag inc)))

(defn interpret-bulk-operation [{:keys [tx last-synctag] :as state} op]
  (assoc state :tx 
    (case (:operation op)
      :docs-delete (docs/delete-document tx (:id op))
      :docs-put (docs/store-document tx (:id op) (:document op) 
                                   (next-synctag last-synctag)))))

(def default-query {
                    :index "default"
                    :wait-duration 5
                    :wait false
                    :query "*"
                    :sort-order :asc
                    :sort-by nil
                    :offset 0
                    :amount 1000
                    })
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
             :base-vclock base-vclock)
      (f)
      (docs/write-last-synctag @last-synctag) 
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

(defn check-document-write [tx id metadata success conflict]
  (let [history-result 
        (checked-history tx
            (:history metadata) (:history (or (docs/load-document-metadata tx id) {})))]
         (if (:is-conflict history-result)
           (conflict metadata)
           (success (assoc metadata :history (:history history-result))))))

(defn put-document 
  ([instance id document] (put-document instance id document {}))
  ([{:keys [last-synctag] :as instance} id document metadata]
  (debug "putting a document:" id document metadata)
   (in-tx instance 
     (fn [tx]
       (check-document-write 
         tx id metadata
         #(docs/store-document tx id document (next-synctag last-synctag) %1)
         #(docs/store-conflict tx id document (next-synctag last-synctag) %1))))))

(defn delete-document 
  ([instance id] (delete-document instance id nil))
  ([{:keys [last-synctag] :as instance} id metadata]
  (debug "deleting a document with id " id)
   (in-tx instance 
     (fn [tx]
       (check-document-write 
         tx id metadata
         #(docs/delete-document tx id (next-synctag last-synctag) %1)
         #(docs/store-conflict tx id :deleted (next-synctag last-synctag) %1))))))

(defn load-document 
  [{:keys [storage]} id]
  (debug "getting a document with id " id)
  (docs/load-document storage id))

(defn bulk 
  [{:keys [last-synctag] :as instance} operations]
  (debug "Bulk operation: ")
  (in-tx instance 
    (fn [tx] (:tx 
    (reduce
      interpret-bulk-operation
      {:tx tx :last-synctag last-synctag}
      operations)))))

(defn put-index 
  [{:keys [last-synctag] :as instance} index]
  (debug "putting an in index:" index)
  (in-tx instance 
    (fn [tx] 
      (indexes/put-index tx index (next-synctag last-synctag)))))

(defn load-index-metadata
  [{:keys [storage]} id]
  (debug "getting index metadata id " id)
  {:synctag (indexes/synctag-for-index storage id)})

(defn delete-index 
  [{:keys [storage last-synctag] :as instance} id]
  (debug "deleting an index" id)
  (in-tx instance 
    (fn [tx] (indexes/delete-index tx id (next-synctag last-synctag)))))

(defn load-index 
  [{:keys [storage]} id]
  (debug "getting an index with id " id)
  (with-open [tx (s/ensure-transaction storage)]
    (indexes/load-index tx id)))
