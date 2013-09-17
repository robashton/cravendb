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

(defn put-into-index! [index doc-id mapped]
  (.put-entry! index doc-id mapped))

(defn flush-index! [index]
  (.flush! index))

(defn process-mapped-document! 
  [ {:keys [max-etag tx doc-count] :as output} 
    {:keys [etag index-id id mapped]}] 
  (if mapped
    (-> output
      (update-in [:indexes index-id :writer] put-into-index! id mapped)
      (assoc :max-etag (newest-etag max-etag etag))
      (assoc :doc-count (inc doc-count)))  
    output))

(defn process-mapped-documents! [tx indexes results] 
  (reduce process-mapped-document! 
          {:indexes  (into {} (for [i indexes] [ (i :id) i])) 
           :max-etag (zero-etag) 
           :tx tx 
           :doc-count 0} results))

(defn finish-map-process! [{:keys [indexes max-etag tx doc-count]}]
  (doseq [[k v] indexes] (update-in v [:writer] flush-index!))
  (-> tx
    (.store last-indexed-etag-key max-etag)
    (.store last-index-doc-count-key doc-count)
    (.commit!)))

(defn prepare-indexes [indexes]
 (for [index indexes] (do
      (assoc index :writer (.open-writer (index :storage))))))

(defn destroy-indexes [indexes]
   (doseq [index indexes]
     (do
       (.close (index :writer)))))

(defn index-documents! [db indexes]
  (with-open [tx (.ensure-transaction db)]
    (with-open [iter (.get-iterator tx)]
      (let [compiled-indexes (prepare-indexes indexes)] ;; This needs handing MUCH better
        (try
          (->> 
            (last-indexed-etag tx)
            (docs/iterate-etags-after iter)
            (index-docs tx indexes)
            (process-mapped-documents! tx compiled-indexes)
            (finish-map-process!))
          (finally (destroy-indexes compiled-indexes)))))))


(defn start-background-indexing [db]
  (future 
    (loop []
      (Thread/sleep 100)
      (try
        (index-documents! db (indexes/load-compiled-indexes db))
        (catch Exception e
          (error e)))
      (recur))))
