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

(defn perform-query 
  [tx reader query offset amount sort-field sort-order]
  (loop [results ()
         current-offset offset
         total-collected 0
         attempt 0 ]
         (let [requested-amount (+ current-offset (max amount 100))
               raw-results (lucene/query reader query requested-amount sort-field sort-order)
               document-results (convert-results-to-documents tx (drop current-offset raw-results))
               new-results (take amount (concat results document-results))
               new-total (count new-results) 
               new-offset (+ current-offset requested-amount)]

           (debug "Requested" requested-amount 
                    "CurrentTotal" total-collected 
                    "Skipped" current-offset "Of"
                    "Received" (count raw-results))
           (if (and (not= (count raw-results) 0)
                    (not= new-total amount)
                    (> 10 attempt))
             (recur new-results 
                    new-offset 
                    new-total
                    (inc attempt))
             new-results))))

(declare execute)

(defn query-with-storage [db storage query]
  (with-open [reader (lucene/open-reader storage)
              tx (s/ensure-transaction db)]
  (perform-query 
    tx
    reader 
    (:query query)
    (or (:offset query) 0)
    (or (:amount query) 1000)
    (:sort-by query)
    (or (:sort-order query) :asc))))

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
