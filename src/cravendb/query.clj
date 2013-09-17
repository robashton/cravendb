(ns cravendb.query
  (require    
    [cravendb.documents :as docs]
    [cravendb.indexes :as indexes]))


(defn execute [tx ops] tx)

;; (defn execute
;;   [tx ops]
;;   (with-open
;;     [reader (.open-reader )]
;;        (doall (map (partial docs/load-document tx) (.query reader { :query "author:vicky"})))) 
;;   )
