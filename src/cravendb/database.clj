(ns cravendb.database
  (:require [cravendb.storage :as s]
            [cravendb.indexing :as indexing] 
            [cravendb.query :as query] 
            [cravendb.indexstore :as indexes] 
            [cravendb.indexengine :as indexengine] 
            [cravendb.documents :as docs]
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
           :last-synctag (atom (synctag-to-integer (docs/last-synctag-in storage))))))

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

(defn is-conflict [session id current-synctag]
  (and current-synctag (not= current-synctag (docs/synctag-for-doc session id))))

(defn clear-conflicts [{:keys [storage]} id]
  (with-open [tx (s/ensure-transaction storage)] 
    (s/commit! (docs/without-conflicts tx id))))

(defn in-tx [{:keys [storage last-synctag] :as instance} f]
  (with-open [tx (s/ensure-transaction storage)] 
    (s/commit!
      (docs/write-last-synctag (f tx) @last-synctag))))

(defn put-document 
  ([instance id document] (put-document instance id document nil))
  ([{:keys [last-synctag] :as instance} id document known-synctag]
  (debug "putting a document:" id document known-synctag)
   (in-tx instance 
     (fn [tx]
       (if (is-conflict tx id known-synctag)
         (docs/store-conflict tx id document known-synctag (next-synctag last-synctag))
         (docs/store-document tx id document (next-synctag last-synctag)))))))

(defn delete-document 
  ([instance id] (delete-document instance id nil))
  ([{:keys [last-synctag] :as instance} id known-synctag]
  (debug "deleting a document with id " id)
  (in-tx instance 
     (fn [tx]
       (if (is-conflict tx id known-synctag)
         (docs/store-conflict tx id :deleted known-synctag (next-synctag last-synctag))
         (docs/delete-document tx id (next-synctag last-synctag)))))))

(defn load-document 
  [{:keys [storage]} id]
  (debug "getting a document with id " id)
  (docs/load-document storage id))

(defn load-document-metadata
  [{:keys [storage]} id]
  (debug "getting document metadata id " id)
  {:synctag (docs/synctag-for-doc storage id)})

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
  


