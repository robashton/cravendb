(ns cravendb.indexing
  (use [cravendb.core]
       [clojure.tools.logging :only (info error)])
  (:require [cravendb.storage :as storage]
            [cravendb.indexstore :as indexes]
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

(defn wait-for-index-catch-up [db]
  (let [last-etag (etag-to-integer (docs/last-etag db))]
   (while (> last-etag (etag-to-integer (last-indexed-etag db)))
     (Thread/sleep 100))))

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

(defn put-into-writer! [writer doc-id mapped]
  (.put-entry! writer doc-id mapped))

(defn process-mapped-document! 
  [ {:keys [max-etag tx doc-count] :as output} 
    {:keys [etag index-id id mapped]}] 
  (if mapped
    (-> output
      (update-in [:writers index-id] put-into-writer! id mapped)
      (assoc :max-etag (newest-etag max-etag etag))
      (assoc :doc-count (inc doc-count)))  
    output))

(defn process-mapped-documents! [tx compiled-indexes results] 
  (reduce process-mapped-document! 
          {:writers (into {} (for [i compiled-indexes] [ (i :id) (i :writer)])) 
           :max-etag (zero-etag) 
           :tx tx 
           :doc-count 0} results))

(defn finish-map-process! [{:keys [writers max-etag tx doc-count]}]
  (doseq [[k v] writers] (.flush! v))
  (-> tx
    (.store last-indexed-etag-key max-etag)
    (.store last-index-doc-count-key doc-count)
    (.commit!)))

(defn index-documents! [db compiled-indexes]
  (with-open [tx (.ensure-transaction db)]
    (with-open [iter (.get-iterator tx)]
      (->> 
        (last-indexed-etag tx)
        (docs/iterate-etags-after iter)
        (index-docs tx compiled-indexes)
        (process-mapped-documents! tx compiled-indexes)
        (finish-map-process!)))))

