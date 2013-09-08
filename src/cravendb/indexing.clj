(ns cravendb.indexing
  (:require [cravendb.storage :as storage]
            [cravendb.documents :as docs]))

(defn last-indexed-etag [db]
  (or (.get-integer db "last-indexed-etag") 0))

(defn load-document-for-indexing [tx id]
  {
   :doc (read-string (docs/load-document tx id))
   :id id
   })

(defn index-docs [tx indexes ids]
  (for [item (map (partial load-document-for-indexing tx) ids)
        index indexes]
    {
     :id (item :id)
     :index (index :name)
     :mapped ((index :map) (item :doc)) 
    }))

(defn index-documents [db indexes]
  (with-open [tx (.ensure-transaction db)]
    (with-open [iter (.get-iterator tx)]
      (->> (last-indexed-etag tx)
           (docs/iterate-etags-after iter)
           (index-docs tx indexes)
           (process-mapped-documents tx)))))

#_ (def storage (storage/create-storage "testdir"))
#_ (.close storage)
#_ (with-open [tx (.ensure-transaction storage)]
     (-> tx
       (docs/store-document "1" (pr-str { :title "hello" :author "rob"}))
       (docs/store-document "2" (pr-str { :title "morning" :author "vicky"}))
       (docs/store-document "3" (pr-str { :title "goodbye" :author "james"}))
       (.commit)))


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

#_ (bindex-documents storage (get-indexes))

#_ (with-open [tx (.ensure-transaction storage)]
    (last-indexed-etag tx))

#_ (defn print-doc [doc]
     (println "zomg" doc))

#_ (with-open [tx (.ensure-transaction storage)]
     (docs/load-document tx "1"))

#_ (with-open [tx (.ensure-transaction storage)]
     (docs/last-etag tx))


#_ (with-open [tx (.ensure-transaction storage)]
     (with-open [iter (.get-iterator tx)]
       (index-docs tx (docs/iterate-etags-after 0 iter)))) 

#_ (with-open [tx (.ensure-transaction storage)]
     (with-open [iter (.get-iterator tx)]
      (index-docs tx (docs/iterate-etags-after iter 0))))

#_ (def mylist '(0 1 2 3 4 5))
#_ (doseq [x mylist] (println x))

