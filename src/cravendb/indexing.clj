(ns cravendb.indexing
  (:use [cravendb.core]
       [clojure.tools.logging :only (info error debug)])
  (:require [cravendb.storage :as storage]
            [clj-time.core :as tc]
            [clj-time.local :as tl]
            [cravendb.storage :as s]
            [cravendb.lucene :as lucene]
            [cravendb.indexstore :as indexes]
            [cravendb.documents :as docs])) 

(def last-indexed-etag-key "last-indexed-etag")
(def last-index-doc-count-key "last-index-doc-count")

(defn last-indexed-etag [db]
  (or (s/get-string db last-indexed-etag-key) (zero-etag)))

(defn last-index-doc-count [db]
    (s/get-integer db last-index-doc-count-key))

(defn load-document-for-indexing [tx id] 
  (debug "Loading " id "for indexing") 
    { 
      :doc (docs/load-document tx id) 
      :id id
      :etag (docs/etag-for-doc tx id)
    })

(defn wait-for-index-catch-up 
  ([db] (wait-for-index-catch-up db 5))
  ([db timeout]
   (let [last-etag (etag-to-integer (docs/last-etag-in db))
          start-time (tl/local-now) ]
     (debug "starting waiting for index catch-up" last-etag)
    (while (and
              (> timeout (tc/in-seconds (tc/interval start-time (tl/local-now))))
              (> last-etag (etag-to-integer (last-indexed-etag db))))

      (debug "looping for index catch-up" last-etag (last-indexed-etag db))
      (Thread/sleep 100))))
   ([db index-id timeout]
    (let [last-etag (etag-to-integer (docs/last-etag-in db))
          start-time (tl/local-now) ]
    (while (and
              (> timeout (tc/in-seconds (tc/interval start-time (tl/local-now))))
              (> last-etag (etag-to-integer (indexes/get-last-indexed-etag-for-index db index-id))))
      (Thread/sleep 100))))) 

(defn apply-map-to-document [index doc id]
  (if (not doc) nil
   (if (and (:filter index) (not ((:filter index) doc { :id id})))
    (do (debug "Skipping " id "because of filter on " (:id index)) nil)
    (try ((:map index) doc)
      (catch Exception ex
        (debug "Failed to index " id "because of" ex) 
        { :__exception ex}
        ))))) 

(defn index-docs [tx indexes ids]
  (debug "indexing documents with indexes" (map :id indexes))
  (if (or (empty? ids) (empty? indexes))
    (do (debug "Idle indexing process") ())
    (do
      (debug "Performing indexing task on stale documents")
      (for [item (map (partial load-document-for-indexing tx) ids)
            index indexes] 
        (do        
          (debug "indexing " (item :id) "with" (index :id))
          {
          :id (item :id)
          :etag (item :etag)
          :index-id (:id index)
          :mapped (apply-map-to-document index (:doc item) (item :id))
          })
          ))))

(defn put-into-writer [writer doc-id mapped]
  (lucene/put-entry writer doc-id mapped))

(defn delete-from-writer [writer doc-id]
  (lucene/delete-all-entries-for writer doc-id))

(defn process-mapped-document 
  [ output {:keys [etag index-id id mapped]}] 
  (-> 
    (cond
      (not mapped) (update-in output [:writers index-id] delete-from-writer id)
      (not (:__exception mapped))
        (-> output 
          (update-in [:writers index-id] delete-from-writer id)
          (update-in [:writers index-id] put-into-writer id mapped)
          (update-in [:stats index-id :index-count] inc )) 
      :else 
      (-> output
        (update-in [:stats index-id :error-count] inc)
        (update-in [:stats index-id :errors] conj (ex-expand (:__exception mapped)))))
    (update-in [:max-etag] (partial newest-etag etag))
    (update-in [:doc-count] inc)
    (update-in [:stats index-id :total-docs] inc)
    ((:pulsefn output))))

(defn create-index-stats [i]
  [(:id i) 
   { :index-count 0
     :error-count 0
     :total-docs 0 
     :errors ()
    }])

(defn process-mapped-documents [tx compiled-indexes pulsefn results] 
  (debug "About to reduce")
  (pulsefn 
    (reduce process-mapped-document 
          {:writers (into {} (map (juxt :id :writer) compiled-indexes)) 
           :stats (into {} (map create-index-stats compiled-indexes))
           :max-etag (last-indexed-etag tx) 
           :tx tx 
           :doc-count 0
           :pulsefn pulsefn
           } results)
    true))

(defn mark-index-as-failed-maybe [tx index-id stats]
  (if (> (/ (:error-count stats) (:total-docs stats)) 0.8)
    (indexes/mark-failed tx index-id stats) tx))

(defn finish-map-process-for-writer! [{:keys [max-etag tx stats] :as output} writer]
  (lucene/commit! (get writer 1))
  (assoc output :tx
    (-> tx
      (indexes/set-last-indexed-etag-for-index (get writer 0) max-etag)
      (mark-index-as-failed-maybe (get writer 0) (stats (get writer 0))))))

(defn finish-map-process! 
  ([output] (finish-map-process! output false))
  ([{:keys [writers max-etag tx doc-count stats] :as output} force-flush]
  (if (and (< 0 doc-count) (or force-flush (= 0 (mod doc-count 1000))))
    (do 
      (debug "Flushing main map process at " doc-count max-etag)
      (-> (:tx (reduce finish-map-process-for-writer!
                       {:tx tx :max-etag max-etag :stats stats} writers))
        (s/store last-indexed-etag-key max-etag)
        (s/store last-index-doc-count-key doc-count)
        (s/commit!))))
   output))

(defn finish-partial-map-process! 
  ([output] (finish-partial-map-process! output false))
  ([{:keys [writers max-etag tx doc-count stats] :as output} force-flush]
  (if (and (< 0 doc-count) (or force-flush (= 0 (mod doc-count 1000))))
    (do (debug "Flushing chaser process at " doc-count max-etag)
      (s/commit! 
        (:tx (reduce finish-map-process-for-writer! 
               {:tx tx :max-etag max-etag :stats stats} writers)))))
    output))

(defn index-documents-from-etag! [tx indexes etag pulsefn]
  (with-open [iter (s/get-iterator tx)] 
    (let [valid-indexes (filter #(not (indexes/is-failed tx (:id %1))) indexes)] 
      (->> (take 10000 (docs/iterate-etags-after iter etag)) 
        (index-docs tx valid-indexes)
        (process-mapped-documents tx valid-indexes pulsefn)))) )

(defn index-catchup! [db index]
  (with-open [tx (s/ensure-transaction db)]
    (let [last-etag (indexes/get-last-indexed-etag-for-index tx (:id index))]
      (index-documents-from-etag! tx [index] last-etag finish-partial-map-process!))))

(defn index-documents! [db compiled-indexes]
  (with-open [tx (s/ensure-transaction db)]
    (let [last-etag (last-indexed-etag tx)]
      (index-documents-from-etag! tx compiled-indexes last-etag finish-map-process!))))
