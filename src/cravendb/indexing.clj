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
  (debug "Loading " id "for indexing") 
    { 
      :doc (read-string (docs/load-document tx id))
      :id id
      :etag (docs/etag-for-doc tx id)
    })

(defn wait-for-index-catch-up 
  ([db] (wait-for-index-catch-up db 5))
  ([db timeout]
   (let [last-etag (etag-to-integer (docs/last-etag db))
          start-time (tl/local-now) ]
    (while (and
              (> timeout (tc/in-seconds (tc/interval start-time (tl/local-now))))
              (> last-etag (etag-to-integer (last-indexed-etag db))))
      (Thread/sleep 100))))
   ([db index-id timeout]
   (let [last-etag (etag-to-integer (docs/last-etag db))
          start-time (tl/local-now) ]
    (while (and
              (> timeout (tc/in-seconds (tc/interval start-time (tl/local-now))))
              (> last-etag (etag-to-integer (indexes/get-last-indexed-etag-for-index db index-id))))
      (Thread/sleep 100))))) 


(defn index-docs [tx indexes ids]
  (debug "indexing documents with indexes" (map :id indexes))
  (if (or (empty? ids) (empty? indexes))
    (do
      (debug "Idle indexing process")
      ()
      )
    (do
      (debug "Performing indexing task on stale documents")
      (for [item (map (partial load-document-for-indexing tx) ids)
          index indexes] 
        (try
          (debug "indexing " (item :id) "with" (index :id))
          {
            :id (item :id)
            :etag (item :etag)
            :index-id (index :id) 
            :mapped ((index :map) (item :doc)) 
          }
          (catch Exception e
            (error "Indexing document failed" (:id item) (:id index))
            {
             :id (item :id)
             :etag (item :etag)
             :index-id (index :id)
             }
            )
          )))))

(defn put-into-writer [writer doc-id mapped]
  (.put-entry writer doc-id mapped))

(defn delete-from-writer [writer doc-id]
  (.delete-all-entries-for writer doc-id))

(defn process-mapped-document 
  [ {:keys [max-etag tx doc-count] :as output} 
    {:keys [etag index-id id mapped]}] 
  (-> 
    (if mapped
      (do
        (-> ((:pulsefn output) output)
          (update-in [:writers index-id] delete-from-writer id)
          (update-in [:writers index-id] put-into-writer id mapped)))
        output)
      (assoc :max-etag (newest-etag max-etag etag))
      (assoc :doc-count (inc doc-count)) ))

(defn process-mapped-documents [tx compiled-indexes pulsefn results] 
  (debug "About to reduce")
  (pulsefn 
    (reduce process-mapped-document 
          {:writers (into {} (for [i compiled-indexes] [ (i :id) (i :writer)])) 
           :max-etag (last-indexed-etag tx) 
           :tx tx 
           :doc-count 0
           :pulsefn pulsefn
           } results)
    true))

(defn finish-map-process-for-writer! [{:keys [max-etag tx] :as output} writer]
  (.commit! (get writer 1))
  (assoc output :tx 
    (indexes/set-last-indexed-etag-for-index tx (get writer 0) max-etag)))

(defn finish-map-process! 
  ([output] (finish-map-process! output false))
  ([{:keys [writers max-etag tx doc-count] :as output} force-flush]
  (if (and (< 0 doc-count) (or force-flush (= 0 (mod doc-count 1000))))
    (do (debug "Flushing main map process at " doc-count max-etag)
      (-> (:tx (reduce finish-map-process-for-writer! {:tx tx :max-etag max-etag} writers))
        (.store last-indexed-etag-key max-etag)
        (.store last-index-doc-count-key doc-count)
        (.commit!))))
   output)) 

(defn finish-partial-map-process! 
  ([output] (finish-partial-map-process! output false))
  ([{:keys [writers max-etag tx doc-count] :as output} force-flush]
  (if (and (< 0 doc-count) (or force-flush (= 0 (mod doc-count 1000))))
    (do (debug "Flushing chaser process at " doc-count max-etag)
      (.commit! (:tx (reduce finish-map-process-for-writer! {:tx tx :max-etag max-etag} writers)))))
    output))

(defn index-documents-from-etag! [tx indexes etag pulsefn]
  (with-open [iter (.get-iterator tx)] 
    (->> (take 10000 (docs/iterate-etags-after iter etag)) 
          (index-docs tx indexes)
          (process-mapped-documents tx indexes pulsefn))) )

(defn index-catchup! [db index]
  (with-open [tx (.ensure-transaction db)]
    (let [last-etag (indexes/get-last-indexed-etag-for-index tx (:id index))]
      (index-documents-from-etag! tx [index] last-etag finish-partial-map-process!))))

(defn index-documents! [db compiled-indexes]
  (with-open [tx (.ensure-transaction db)]
    (let [last-etag (last-indexed-etag tx)]
      (index-documents-from-etag! tx compiled-indexes last-etag finish-map-process!))))
