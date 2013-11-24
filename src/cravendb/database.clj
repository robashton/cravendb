(ns cravendb.database
  (:require [cravendb.storage :as s]
            [cravendb.indexing :as indexing] 
            [cravendb.query :as query] 
            [cravendb.indexstore :as indexes] 
            [cravendb.indexengine :as ie] 
            [cravendb.documents :as docs]
            [cravendb.inflight :as inflight]
            [cravendb.vclock :as vclock]
            [clojure.tools.logging :refer [info error debug]]))

(defrecord Database [storage index-engine ifh]
  java.io.Closeable
  (close [this] 
    (ie/stop index-engine)
    (.close index-engine)
    (.close storage)))

(defn open-storage [opts]
  (if (:path opts) (s/create-storage (:path opts)) (s/create-in-memory-storage)))

(defn creation-options [kvs]
  (merge { :href nil :path nil } (apply hash-map kvs)))

(defn create-embedded [opts]
  (let [storage (open-storage opts)
        index-engine (ie/create storage)]
    (ie/start index-engine) 
    (merge (Database. storage index-engine 
                      (inflight/create storage (or (:server-id opts) "root"))))))

(defn create-remote [opts]
  { :href (:href opts)})

(defn create
  "Creates a database for use in the application
  Possible options are
    :path is where to store the data, this is for embedded databases
    :href is the location of the remote database
  if no :path or :href is specified, then it is assumed you want an in-memory database"
  [& kvs]
  (let [opts (creation-options kvs)]
    (if (:href opts) 
      (create-remote opts)
      (create-embedded opts))))

(defn load-document-metadata
  [{:keys [storage]} id]
  (debug "getting document metadata id " id)
  (docs/load-document-metadata storage id))

(def default-query { :index "default"
                     :wait-duration 5
                     :wait false
                     :filter "*"
                     :sort-order :asc
                     :sort-by nil
                     :offset 0
                     :amount 1000 })
(defn query
  "Queries the database with the options specified in & kvs
  The available options are
  :index \"default\" - The index to use for this query *advanced*
  :wait-duration 5   - How long to wait for the index to become non-stale (if wait is true)
  :wait false        - Whether to wait for the index to become non-stale
  :filter \"*\"      - The filter to apply to the query (by default, everything)
  :sort-order :asc   - The default sort-order (ascending)
  :sort-by nil       - The field to sort by (default, by best-match)
  :offset 0          - For paging, how many results to skip
  :amount 1000       - For paging, how many results to request"
  [{:keys [storage index-engine]} & kvs]
  (debug "Querying for " kvs)
  (query/execute storage index-engine (merge default-query (apply hash-map kvs))))

(defn clear-conflicts [{:keys [storage]} id]
  (with-open [tx (s/ensure-transaction storage)] 
    (s/commit! (docs/without-conflicts tx id))))

(defn conflicts [{:keys [storage]}]
  (docs/conflicts storage))

(defn put-document 
  ([instance id document] (put-document instance id document {}))
  ([{:keys [ifh index-engine]} id document metadata]
  (debug "putting a document:" id document metadata)
   (let [txid (inflight/open ifh)]
     (inflight/add-document ifh txid id document metadata)
     (inflight/complete! ifh txid)
     (ie/notify-of-work index-engine))))

(defn delete-document 
  ([instance id] (delete-document instance id nil))
  ([{:keys [ifh index-engine]} id metadata]
  (debug "deleting a document with id " id)
   (let [txid (inflight/open ifh)]
     (inflight/delete-document ifh txid id metadata)
     (inflight/complete! ifh txid)
     (ie/notify-of-work index-engine))))

(defn load-document 
  [{:keys [storage]} id]
  (debug "getting a document with id " id)
  (docs/load-document storage id))

(defn bulk 
  [{:keys [ifh index-engine]} operations]
  (debug "Bulk operation: ")
  (let [txid (inflight/open ifh)]
    (doseq [{:keys [id operation metadata document]} operations]
      (case operation
        :docs-delete (inflight/delete-document ifh txid id metadata)
        :docs-put (inflight/add-document ifh txid id document metadata)))
     (inflight/complete! ifh txid)
    (ie/notify-of-work index-engine)))

(defn put-index 
  [{:keys [storage index-engine]} index]
  (debug "putting an in index:" index)
  (with-open [tx (s/ensure-transaction storage)] 
    (s/commit! (indexes/put-index tx index {:synctag (s/next-synctag tx)})))
  (ie/notify-of-new-index index-engine index))

(defn load-index-metadata
  [{:keys [storage]} id]
  (debug "getting index metadata id " id)
  {:synctag (indexes/synctag-for-index storage id)})

(defn delete-index 
  [{:keys [storage index-engine]} id]
  (debug "deleting an index" id)
  (with-open [tx (s/ensure-transaction storage)] 
    (s/commit! (indexes/delete-index tx id {:synctag (s/next-synctag tx)})))
  (ie/notify-of-removed-index index-engine id))

(defn load-index 
  [{:keys [storage]} id]
  (debug "getting an index with id " id)
  (with-open [tx (s/ensure-transaction storage)]
    (indexes/load-index tx id)))

