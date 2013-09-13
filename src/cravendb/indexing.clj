(ns cravendb.indexing
  (use [cravendb.core])
  (:require [cravendb.storage :as storage]
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
  (for [item (map (partial load-document-for-indexing tx) ids)
        index indexes] {
     :id (item :id)
     :etag (item :etag)
     :index (index :name)
     :mapped ((index :map) (item :doc)) 
    }))

(defn process-mapped-document [{:keys [max-etag tx doc-count] :as output} {:keys [etag index id mapped]}] 
  (-> output
    (assoc :max-etag (newest-etag max-etag etag))
    (assoc :doc-count (inc doc-count))
    (assoc :tx (.store tx (str "index-result-" index id) (pr-str mapped)))))

(defn process-mapped-documents [tx results] 
  (reduce process-mapped-document {:max-etag (zero-etag) :tx tx :doc-count 0} results))

(defn finish-map-process! [{:keys [max-etag tx doc-count]}]
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
           (process-mapped-documents tx)
           (finish-map-process!)))))
