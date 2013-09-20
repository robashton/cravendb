(ns cravendb.query
  (require    
    [cravendb.documents :as docs]))

(defn execute [db indexes query]
  (with-open [reader (.open-reader-for indexes (query :index))]
    (doall (map (partial docs/load-document db) (.query reader query )))))
