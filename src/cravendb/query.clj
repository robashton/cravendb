(ns cravendb.query
  (use [cravendb.core]
       [clojure.tools.logging :only (info error debug)])
  (require    
    [cravendb.indexing :as indexing]
    [cravendb.documents :as docs]))

(defn convert-results-to-documents [tx results]
  (filter boolean (map (partial docs/load-document tx) results)))

(defn perform-query [tx reader query offset amount sort-field sort-order]
  (loop [results ()
         current-offset offset
         total-collected 0
         attempt 0 ]
         (let [requested-amount (+ current-offset (max amount 100))
               raw-results (.query reader query requested-amount sort-field sort-order)
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

(defn execute [db index-engine query]
  (if (query :wait) (indexing/wait-for-index-catch-up 
                      db 
                      (:index query) 
                      (or (:wait-duration query) 5)))
  (with-open [reader (.open-reader index-engine (:index query))
              tx (.ensure-transaction db)]
    (perform-query tx
                   reader 
                   (:query query)
                   (or (:offset query) 0)
                   (or (:amount query) 1000)
                   (:sort-by query)
                   (or (:sort-order query) :asc))))

