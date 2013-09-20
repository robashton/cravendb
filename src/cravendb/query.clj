(ns cravendb.query
  (require    
    [cravendb.indexing :as indexing]
    [cravendb.documents :as docs]))

(defn execute [db indexes query]
  (if (query :wait) (indexing/wait-for-index-catch-up db))
  (with-open [reader (.open-reader-for indexes (query :index))]
    (doall 
      (map 
        (partial docs/load-document db) 
        (.query reader query )))))
