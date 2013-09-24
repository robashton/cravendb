(ns cravendb.query
  (require    
    [cravendb.indexing :as indexing]
    [cravendb.documents :as docs]))

(defn execute [db index-engine query]
  (if (query :wait) (indexing/wait-for-index-catch-up db))
  (with-open [reader (.open-reader index-engine (query :index))]
    (doall 
      (filter boolean 
        (map 
          (partial docs/load-document db) 
            (.query reader query ))))))
