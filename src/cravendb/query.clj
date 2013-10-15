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

(defn create-producer [tx reader query sort-field sort-order]
  (fn [offset amount]
    (convert-results-to-documents tx
      (drop offset (lucene/query reader query (+ offset amount) sort-field sort-order)))))

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

(defn perform-query 
  [producer offset amount]
  (doall
    (take amount 
      (drop offset 
        (result-seq (paged-results producer (+ offset amount)))))))

(declare execute)

(defn query-with-storage [db storage query]
  (try
    (with-open [reader (lucene/open-reader storage)
              tx (s/ensure-transaction db)]
    (perform-query 
      (create-producer 
        tx
        reader 
        (:query query) 
        (:sort-by query) 
        (or (:sort-order query) :asc))
      (or (:offset query) 0)
      (or (:amount query) 1000)))
    (catch Exception ex ;; TODO: Be more specific
      (info "Failed to query with" query "because" ex)
      ())))

(defn wait-for-new-index [db index-engine query]
  (execute db index-engine (assoc query :wait 5)))

(defn query-without-storage [db index-engine query]
  (if (indexes/load-index db (:index query))
      (wait-for-new-index db index-engine query)
      nil))

(defn execute [db index-engine query]
  (if (query :wait) 
    (indexing/wait-for-index-catch-up 
      db 
      (or (:index query) "default") 
      (or (:wait-duration query) 5)))
  (let [storage (indexengine/get-index-storage index-engine (:index query))]
    (if storage 
      (query-with-storage db storage query)
      (query-without-storage db index-engine query))))
