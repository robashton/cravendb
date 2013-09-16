(ns cravendb.indexing
  (use [cravendb.core]
       [clojure.tools.logging :only (info error)])
  (:require [cravendb.storage :as storage]
            [cravendb.indexes :as indexes]
            [cravendb.documents :as docs])) 

(def last-indexed-etag-key "last-indexed-etag")
(def last-index-doc-count-key "last-index-doc-count")

(defn last-indexed-etag [db]
  (or (.get-string db last-indexed-etag-key) (zero-etag)))

(defn last-index-doc-count [db]
    (.get-integer db last-index-doc-count-key))

(defn load-document-for-indexing [tx id] {
   :doc (read-string (docs/load-document tx id))
   :id id
   :etag (docs/etag-for-doc tx id)
   })

(defn index-docs [tx indexes ids]
  (if (empty? ids)
    ()
    (do
      (info "Performing indexing task on stale documents")
      (for [item (map (partial load-document-for-indexing tx) ids)
          index indexes] 
        (try
          {
            :id (item :id)
            :etag (item :etag)
            :index-id (index :id) 
            :mapped ((index :map) (item :doc)) 
          }
          (catch Exception e
            ;; TODO: Log this
            (println "LOL")
            nil
            )
          )))))

(defn open-writers-for-indexes
  [indexes]
  (into {} (for [i indexes] 
             [(i :id) (.open-writer (i :storage))])))

(defn process-mapped-document 
  [ {:keys [max-etag tx doc-count] :as output} 
    {:keys [etag index-id id mapped]}] 
  (if mapped
    (-> (assoc-in 
          output 
          [:writers index-id] 
          (.put-entry! (get-in output [:writers index-id]) id mapped))
      (assoc :max-etag (newest-etag max-etag etag))
      (assoc :doc-count (inc doc-count)))  
    output))

(defn process-mapped-documents [tx indexes results] 
  (reduce process-mapped-document 
          {:writers (open-writers-for-indexes indexes) 
           :max-etag (zero-etag) 
           :tx tx 
           :doc-count 0} results))

(defn finish-map-process! [{:keys [writers max-etag tx doc-count]}]
  (doseq [[k v] writers] 
    (.close (.commit! v)))
  (-> tx
    (.store last-indexed-etag-key max-etag)
    (.store last-index-doc-count-key doc-count)
    (.commit!)))

(defn index-documents! [db indexes]
  (with-open [tx (.ensure-transaction db)]
    (with-open [iter (.get-iterator tx)]
      (->> (last-indexed-etag tx)
           (docs/iterate-etags-after iter)
           (index-docs tx indexes)
           (process-mapped-documents tx indexes)
           (finish-map-process!)))))

(defn start-background-indexing [db]
  (future 
    (loop []
      (Thread/sleep 100)
      (try
        (index-documents! db (indexes/load-compiled-indexes db)) 
        (catch Exception e
          (error e)))
      (recur))))
