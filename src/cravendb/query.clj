(ns cravendb.query
  (:use [cravendb.core]
       [cravendb.indexstore :as indexes]
       [clojure.tools.logging :only (info error debug)])
  (:require    
    [cravendb.indexing :as indexing]
    [cravendb.indexengine :as indexengine]
    [cravendb.documents :as docs]
    [cravendb.lucene :as lucene]
    [cravendb.storage :as s]))

(defn convert-results-to-documents [tx results]
  (filter boolean (map (partial docs/load-document tx) results)))

(defn create-producer [tx reader opts]
  (fn [offset amount]
    (convert-results-to-documents tx
      (drop offset (lucene/query 
                     reader 
                     (:query opts) 
                     (+ offset amount) 
                     (:sort-by opts) 
                     (:sort-order opts))))))

(defn paged-results 
  ([producer page-size] (paged-results producer 0 page-size))
  ([producer current-offset page-size]
   {
    :results (producer current-offset page-size)
    :next (fn [] (paged-results producer (+ current-offset page-size) page-size))
   }))

(defn result-seq 
  ([page] (result-seq page ()))
  ([page coll] (result-seq page (:results page) coll))
  ([page src coll]
   (cond
     (empty? (:results page)) coll
     (empty? src) (result-seq ((:next page)) coll)
     :else (cons (first src) (lazy-seq (result-seq page (rest src) coll))))))

(declare execute)

(defn query-with-storage [db storage {:keys [offset amount] :as opts}]
  (try
    (with-open [reader (lucene/open-reader storage)
              tx (s/ensure-transaction db)]
      (doall (take amount (drop offset 
        (result-seq 
         (paged-results (create-producer tx reader opts) (+ offset amount)))))))

    (catch Exception ex ;; TODO: Be more specific
      (info "Failed to query with" opts "because" ex)
      ())))

(defn wait-for-new-index [db index-engine query]
  (execute db index-engine (assoc query :wait 5)))

(defn query-without-storage [db index-engine query]
  (if (indexes/load-index db (:index query))
      (wait-for-new-index db index-engine query)
      nil))

(defn execute [db index-engine {:keys [index wait wait-duration] :as opts}]
  (if wait 
    (indexing/wait-for-index-catch-up db index wait-duration))
  (let [storage (indexengine/get-index-storage index-engine index)]
    (if storage 
      (query-with-storage db storage opts)
      (query-without-storage db index-engine opts))))
