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

(def last-indexed-synctag-key "last-indexed-synctag")
(def last-index-doc-count-key "last-index-doc-count")

(defn last-indexed-synctag [db]
  (or (s/get-obj db last-indexed-synctag-key) (zero-synctag)))

(defn last-index-doc-count [db]
    (s/get-obj db last-index-doc-count-key))

(defn load-document-for-indexing [tx id] 
  (debug "Loading " id "for indexing") 
    { 
      :doc (docs/load-document tx id) 
      :id id
      :synctag (docs/synctag-for-doc tx id)
    })

(defn wait-for-index-catch-up 
  ([db] (wait-for-index-catch-up db 1))
  ([db timeout]
   (let [last-synctag (synctag-to-integer (s/last-synctag-in db))
          start-time (tl/local-now) ]
     (debug "starting waiting for index catch-up" last-synctag)
    (while (and
              (> timeout (tc/in-seconds (tc/interval start-time (tl/local-now))))
              (> last-synctag (synctag-to-integer (last-indexed-synctag db))))

      (debug "looping for index catch-up" last-synctag (last-indexed-synctag db))
      (Thread/sleep 100))))
   ([db index-id timeout]
    (let [last-synctag (synctag-to-integer (s/last-synctag-in db))
          start-time (tl/local-now) ]
    (while (and
              (> timeout (tc/in-seconds (tc/interval start-time (tl/local-now))))
              (> last-synctag (synctag-to-integer (indexes/get-last-indexed-synctag-for-index db index-id))))
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
          :synctag (item :synctag)
          :index-id (:id index)
          :mapped (apply-map-to-document index (:doc item) (item :id))
          })
          ))))

(defn put-into-writer [writer doc-id mapped]
  (lucene/put-entry writer doc-id mapped))

(defn delete-from-writer [writer doc-id]
  (lucene/delete-all-entries-for writer doc-id))

(defn process-mapped-document 
  [ output {:keys [synctag index-id id mapped]}] 
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
    (update-in [:max-synctag] (partial newest-synctag synctag))
    (update-in [:doc-count] inc)
    (update-in [:stats index-id :total-docs] inc)))

(defn create-index-stats [i]
  [(:id i) 
   { :index-count 0
     :error-count 0
     :total-docs 0 
     :errors ()
    }])

(defn process-mapped-documents [tx compiled-indexes results] 
  (debug "About to reduce")
  (reduce process-mapped-document 
          {:writers (into {} (map (juxt :id :writer) compiled-indexes)) 
           :stats (into {} (map create-index-stats compiled-indexes))
           :max-synctag (last-indexed-synctag tx) 
           :tx tx 
           :doc-count 0
           } results))

(defn mark-index-as-failed-maybe [tx index-id {:keys [total-docs error-count] :as stats}]
  (if (and (> total-docs 100) (> (/ error-count total-docs) 0.8))
    (do
      (error "marking index as failed" index-id)
      (indexes/mark-failed tx index-id stats)) 
    tx))

(defn finish-map-process-for-writer! [{:keys [max-synctag tx stats] :as output} writer]
  (lucene/commit! (get writer 1))
  (assoc output :tx
    (-> tx
      (indexes/set-last-indexed-synctag-for-index (get writer 0) max-synctag)
      (mark-index-as-failed-maybe (get writer 0) (stats (get writer 0))))))

(defn finish-map-process! 
  [{:keys [writers max-synctag tx doc-count stats] :as output}]
  (do 
    (debug "Flushing main map process at " doc-count max-synctag)
    (-> (:tx (reduce finish-map-process-for-writer!
                     {:tx tx :max-synctag max-synctag :stats stats} writers))
      (s/store last-indexed-synctag-key max-synctag)
      (s/store last-index-doc-count-key doc-count)
      (s/commit!)))
  output)

(defn finish-partial-map-process! 
  [{:keys [writers max-synctag tx doc-count stats] :as output}]
  (do (debug "Flushing chaser process at " doc-count max-synctag)
    (s/commit! 
      (:tx (reduce finish-map-process-for-writer! 
                   {:tx tx :max-synctag max-synctag :stats stats} writers)))))

(defn index-documents-from-synctag! [tx indexes synctag]
  (with-open [iter (s/get-iterator tx)] 
    (let [valid-indexes (filter #(not (indexes/is-failed tx (:id %1))) indexes)
          last-synctag (oldest-synctag 
            (conj (map #(indexes/get-last-indexed-synctag-for-index tx (:id %1)) valid-indexes) 
                  synctag))] 
      (->> (take 1000 (docs/iterate-synctags-after iter last-synctag)) 
        (index-docs tx valid-indexes)
        (process-mapped-documents tx valid-indexes)))) )

(defn index-catchup! [db index]
  (with-open [tx (s/ensure-transaction db)]
    (let [last-synctag (indexes/get-last-indexed-synctag-for-index tx (:id index))]
      (:doc-count 
        (finish-partial-map-process! (index-documents-from-synctag! tx [index] last-synctag))))))

(defn index-documents! [db compiled-indexes]
  (with-open [tx (s/ensure-transaction db)]
    (:doc-count
      (finish-map-process! 
        (index-documents-from-synctag! tx compiled-indexes (last-indexed-synctag tx))))))
