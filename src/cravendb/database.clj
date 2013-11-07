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

(defn create
  [path & kvs]
  (let [storage (s/create-storage path)
        index-engine (ie/create storage)
        opts (apply hash-map kvs) ]
    (ie/start index-engine)
    (merge (Database. storage index-engine 
                      (inflight/create storage (or (:server-id opts) "root"))))))

(defn load-document-metadata
  [{:keys [storage]} id]
  (debug "getting document metadata id " id)
  (docs/load-document-metadata storage id))

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
  ([{:keys [ifh]} id metadata]
  (debug "deleting a document with id " id)
   (let [txid (inflight/open ifh)]
     (inflight/delete-document ifh txid id metadata)
     (inflight/complete! ifh txid))))

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

