(ns cravendb.indexing
  (use [cravendb.core]
       [clojure.tools.logging :only (info error debug)])
  (:require [cravendb.storage :as storage]
            [clj-time.core :as tc]
            [clj-time.local :as tl]
            [cravendb.indexstore :as indexes]
            [cravendb.documents :as docs])) 

(def last-indexed-etag-key "last-indexed-etag")
(def last-index-doc-count-key "last-index-doc-count")

(defn last-indexed-etag [db]
  (or (.get-string db last-indexed-etag-key) (zero-etag)))

(defn last-index-doc-count [db]
    (.get-integer db last-index-doc-count-key))

(defn load-document-for-indexing [tx id] 
   (info "Loading " id "for indexing") {
   :doc (read-string (docs/load-document tx id))
   :id id
   :etag (docs/etag-for-doc tx id)
   })

(defn wait-for-index-catch-up 
  ([db] (wait-for-index-catch-up db 5))
  ([db timeout]
    (let [last-etag (etag-to-integer (with-open [tx (.ensure-transaction db)] (docs/last-etag tx)))
          start-time (tl/local-now) ]
    (while (and
              (> timeout (tc/in-seconds (tc/interval start-time (tl/local-now))))
              (> last-etag (etag-to-integer (with-open [tx (.ensure-transaction db)] (last-indexed-etag tx)))))
      (Thread/sleep 100)))))

(defn index-docs [tx indexes ids]
  (if (empty? ids)
    (do
      (debug "Idle indexing process")
      ()
      )
    (do
      (info "Performing indexing task on stale documents")
      (for [item (map (partial load-document-for-indexing tx) ids)
          index indexes] 
        (try
          (info "indexing " (item :id) "with" (index :id))
          {
            :id (item :id)
            :etag (item :etag)
            :index-id (index :id) 
            :mapped ((index :map) (item :doc)) 
          }
          (catch Exception e
            (info "Indexing document failed" (:id item) (:id index))
            nil
            )
          )))))

(defn put-into-writer [writer doc-id mapped]
  (.put-entry writer doc-id mapped))

(defn delete-from-writer [writer doc-id]
  (.delete-all-entries-for writer doc-id))

(defn process-mapped-document 
  [ {:keys [max-etag tx doc-count] :as output} 
    {:keys [etag index-id id mapped]}] 
  (info "About to send to the writers" index-id id)
  (if mapped
    (-> output
      (update-in [:writers index-id] delete-from-writer id)
      (update-in [:writers index-id] put-into-writer id mapped)
      (assoc :max-etag (newest-etag max-etag etag))
      (assoc :doc-count (inc doc-count)))  
    output))

(defn process-mapped-documents [tx compiled-indexes results] 
  (info "About to reduce")
  (reduce process-mapped-document 
          {:writers (into {} (for [i compiled-indexes] [ (i :id) (i :writer)])) 
           :max-etag (last-indexed-etag tx) 
           :tx tx 
           :doc-count 0} results))

(defn finish-map-process-for-writer! [{:keys [max-etag tx] :as output} writer]
  (.commit! (get writer 1))
  (assoc output :tx 
    (indexes/set-last-indexed-etag-for-index tx (get writer 0) max-etag)))

(defn finish-map-process! [{:keys [writers max-etag tx doc-count]}]
  (-> (:tx (reduce finish-map-process-for-writer! {:tx tx :max-etag max-etag} writers))
    (.store last-indexed-etag-key max-etag)
    (.store last-index-doc-count-key doc-count)
    (.commit!)))


(defn index-documents-from-etag! [tx indexes etag]
  (with-open [iter (.get-iterator tx)] 
    (->>  (docs/iterate-etags-after iter etag)
          (index-docs tx indexes)
          (process-mapped-documents tx indexes)
          (finish-map-process!)))
  (info "Finished indexing bit"))

(defn index-documents! [db compiled-indexes]
  (with-open [tx (.ensure-transaction db)]
    (let [last-etag (last-indexed-etag tx)]
      (index-documents-from-etag! tx compiled-indexes last-etag))))

;;  (filter #(= last-etag (indexes/get-last-indexed-etag-for-index tx (:id %1))))
