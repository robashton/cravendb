(ns cravendb.embedded
  (:require [cravendb.storage :as s]
            [cravendb.indexing :as indexing] 
            [cravendb.query :as q] 
            [cravendb.indexstore :as indexes] 
            [cravendb.indexengine :as ie] 
            [cravendb.documents :as docs]
            [cravendb.inflight :as inflight]
            [cravendb.vclock :as vclock]
            [cravendb.database :refer [DocumentDatabase]]
            [cravendb.stats :as stats]
            [clojure.core.async :refer [chan tap]]
            [clojure.tools.logging :refer [info error debug]]))

(def default-query { :index "default"
                     :wait-duration 5
                     :wait false
                     :filter "*"
                     :sort-order :asc
                     :sort-by nil
                     :offset 0
                     :amount 1000 })

(defrecord EmbeddedDatabase [storage index-engine ifh counters]
  DocumentDatabase
  (close [this] 
    (ie/stop index-engine)
    (.close index-engine)
    (.close storage))

  (load-document-metadata [this id]
    (debug "getting document metadata id " id)
    (docs/load-document-metadata storage id))
  
  (query  
    [this opts]
    (debug "Querying for " opts)
    (q/execute storage index-engine (merge default-query opts)))

  (clear-conflicts [this id]
    (with-open [tx (s/ensure-transaction storage)] 
      (s/commit! (docs/without-conflicts tx id))))

  (conflicts [this] (docs/conflicts storage))

  (put-document [this id document metadata]
    (debug "putting a document:" id document metadata)
    (let [txid (inflight/open ifh)]
      (inflight/add-document ifh txid id document metadata)
      (inflight/complete! ifh txid)
      (ie/notify-of-work index-engine)))

  (delete-document [this id metadata]
    (debug "deleting a document with id " id)
    (let [txid (inflight/open ifh)]
      (inflight/delete-document ifh txid id metadata)
      (inflight/complete! ifh txid)
      (ie/notify-of-work index-engine)))

  (load-document [this id]
    (debug "getting a document with id " id)
    (docs/load-document storage id))

  (bulk [this operations]
    (debug "Bulk operation: ")
    (let [txid (inflight/open ifh)]
      (doseq [{:keys [id operation metadata document]} operations]
        (case operation
          :docs-delete (inflight/delete-document ifh txid id metadata)
          :docs-put (inflight/add-document ifh txid id document metadata)))
      (inflight/complete! ifh txid)
      (ie/notify-of-work index-engine)))

  (put-index [this index]
    (debug "putting an in index:" index)
    (with-open [tx (s/ensure-transaction storage)] 
      (s/commit! (indexes/put-index tx index {:synctag (s/next-synctag tx)})))
    (ie/notify-of-new-index index-engine index))

  (load-index-metadata [this id]
    (debug "getting index metadata id " id)
    {:synctag (indexes/synctag-for-index storage id)})

  (delete-index [this id]
    (debug "deleting an index" id)
    (with-open [tx (s/ensure-transaction storage)] 
      (s/commit! (indexes/delete-index tx id {:synctag (s/next-synctag tx)})))
    (ie/notify-of-removed-index index-engine id))
  
  (load-index [this id]
    (debug "getting an index with id " id)
    (with-open [tx (s/ensure-transaction storage)]
      (indexes/load-index tx id))))

(defn open-storage [opts]
  (if (:path opts) (s/create-storage (:path opts)) (s/create-in-memory-storage)))

(defn create [& kvs]
  (let [opts (apply hash-map kvs)
        storage (open-storage opts)
        index-engine (ie/create storage)
        stats-engine (stats/start) 
        ifh (inflight/create storage (or (:server-id opts) "root"))]
    (ie/start index-engine) 
    (stats/consume stats-engine (tap (:events ifh) (chan)))
    (EmbeddedDatabase. 
      storage 
      index-engine 
      ifh
      stats-engine)))
