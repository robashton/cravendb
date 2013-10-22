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
  (let [storage (s/create-storage path)
        index-engine (indexengine/create-engine storage)]
    (indexengine/start index-engine)
    (assoc (Database. storage index-engine)
           :last-synctag (atom (synctag-to-integer (docs/last-synctag-in storage)))
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

(defn is-conflict [session id metadata]
  false)

(defn clear-conflicts [{:keys [storage]} id]
  (with-open [tx (s/ensure-transaction storage)] 
    (s/commit! (docs/without-conflicts tx id))))

(defn in-tx [{:keys [storage last-synctag] :as instance} f]
  (with-open [tx (s/ensure-transaction storage)] 
    (s/commit!
      (docs/write-last-synctag (f tx) @last-synctag))))

(defn load-document-metadata
  [{:keys [storage]} id]
  (debug "getting document metadata id " id)
  (assoc (docs/load-document-metadata storage id)
         :synctag (docs/synctag-for-doc storage id)))

(defn checked-history [instance id supplied-history existing-history]
  (let [last-version (or existing-history supplied-history (:base-vclock instance))
        next-version (vclock/next 
                       (:server-id instance) (:base-vclock instance) 
                       (or supplied-history existing-history))]
    {:is-conflict (not (vclock/descends? next-version last-version))
     :history next-version}))

(defn put-document 
  ([instance id document] (put-document instance id document {}))
  ([{:keys [last-synctag] :as instance} id document metadata]
  (debug "putting a document:" id document metadata)
   (in-tx instance 
     (fn [tx]
       (let [history-result (checked-history instance id 
                            (:history metadata) (:history (or (load-document-metadata instance id) {})))]
         (if (:is-conflict history-result)
            (docs/store-conflict tx id document (next-synctag last-synctag) metadata)
            (docs/store-document tx id document (next-synctag last-synctag) 
               (assoc metadata :history (:history history-result)))))))))

(defn delete-document 
  ([instance id] (delete-document instance id nil))
  ([{:keys [last-synctag] :as instance} id known-synctag]
  (debug "deleting a document with id " id)
  (in-tx instance 
     (fn [tx]
       (docs/store-conflict tx id :deleted known-synctag (next-synctag last-synctag))))))

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
  


