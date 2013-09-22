(ns cravendb.query
  (require    
    [cravendb.indexing :as indexing]
    [cravendb.indexengine :as indexengine]
    [cravendb.documents :as docs]))

(defn execute [db indexes query]
  (if (query :wait) (indexing/wait-for-index-catch-up db))
  (with-open [reader (indexengine/reader-for-index db (query :index))]
    (doall 
      (map 
     (partial docs/load-document db) 
        (.query reader query )))))
