(ns cravendb.query
  (require    
    [cravendb.indexing :as indexing]
    [cravendb.documents :as docs]))

(defn execute [db index-engine query]
  (if (query :wait) (indexing/wait-for-index-catch-up 
                      db 
                      (:index query) 
                      (or (:wait-duration query) 5)))
  (with-open [reader (.open-reader index-engine (:index query))
              tx (.ensure-transaction db)]
    (doall 
      (filter boolean 
        (map 
          (partial docs/load-document tx) 
            (.query reader query ))))))
