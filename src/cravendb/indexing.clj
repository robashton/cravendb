(ns cravendb.indexing
  (:require [cravendb.storage :as storage]
            [cravendb.documents :as docs]))os 
(defn last-indexed-etag [db]
  (or (.get-string db "last-indexed-etag") (docs/zero-etag)))


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
      (assoc :max-etag (docs/max-etag max-etag etag))
      (assoc :doc-count (inc doc-count))
      (assoc :tx (.store tx (str "index-result-" index id) (pr-str mapped)))))

(defn process-mapped-documents [tx results] 
  (reduce process-mapped-document {:max-etag (docs/zero-etag) :tx tx :doc-count 0} results))

(defn finish-map-process! [{:keys [max-etag tx doc-count]}]
  (-> tx
    (.store "last-indexed-etag" max-etag)
    (.store "last-index-doc-count" doc-count)
    (.commit!)))

(defn index-documents [db indexes]
  (with-open [tx (.ensure-transaction db)]
    (with-open [iter (.get-iterator tx)]
      (->> (last-indexed-etag tx)
           (docs/iterate-etags-after iter)
           (index-docs tx indexes)
           (process-mapped-documents tx)
           (finish-map-process!)))))

#_ (def storage (storage/create-storage "testdb"))
#_ (.close storage)
#_ (with-open [tx (.ensure-transaction storage)]
     (-> tx
       (docs/store-document "1" (pr-str { :title "hello" :author "rob"}))
       (docs/store-document "2" (pr-str { :title "morning" :author "vicky"}))
       (docs/store-document "3" (pr-str { :title "goodbye" :author "james"}))
       (.commit!)))

#_ (defn get-indexes []
     [{
       :name "by_author"
       :map (fn [doc] (doc :author))
       }
      {
       :name "by_title"
       :map (fn [doc] (doc :title))
       }
      ])

#_(with-open [tx (.ensure-transaction storage)]
    (with-open [iter (.get-iterator tx)]
      (.seek iter (storage/to-db (str "etag-docs-")))
        (println (storage/from-db-str (.getKey (.peekNext iter))))))


#_ (index-documents storage (get-indexes))

#_ (with-open [tx (.ensure-transaction storage)]
    (last-indexed-etag tx))

#_ (with-open [tx (.ensure-transaction storage)]
    (.get-integer tx "last-index-doc-count"))

#_ (defn print-doc [tx docs]
     (doseq [i docs]
       (println i)))

#_ (with-open [tx (.ensure-transaction storage)]
     (docs/load-document tx "1"))

#_ (with-open [tx (.ensure-transaction storage)]
     (docs/last-etag tx))

#_ (with-open [tx (.ensure-transaction storage)]
     (docs/etag-for-doc tx "1")) 

#_ (with-open [tx (.ensure-transaction storage)]
     (with-open [iter (.get-iterator tx)]
       (index-docs tx (docs/iterate-etags-after "1" iter)))) 

#_ (with-open [tx (.ensure-transaction storage)]
     (with-open [iter (.get-iterator tx)]
      (print-doc tx (docs/iterate-etags-after iter (docs/integer-to-etag 5)))))

#_ (def mylist '(0 1 2 3 4 5))
#_ (doseq [x mylist] (println x))

(Integer/parseInt (format "%030d" 1))

