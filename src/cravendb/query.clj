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

;;(defn recursive-query
;; ([tx reader query offset amount sort-field sort-order]
;;  (recursive-query tx reader query offset amount sort-field sort-order 0 0 ())) 
;; ([tx reader query offset amount sort-field sort-order counter i coll]
;;  (if (>= counter amount))
;;  ))


;;  ([i coll limit]
;;   (if (> limit i)
;;     (cons (inc i) (lazy-seq (form-sequence (inc i) coll limit))) 
;;     coll)))
;;(defn recursive-query 
;;  ([producer offset amount]
;;   (recursive-query 
;;     producer 
;;     (create-pager producer offset amount) amount 0 ()))
;;  ([producer pager remaining i coll]
;;   (cond
;;     (= remaining 0) coll
;;     (>= i (+ (:offset pager) (:amount pager)))
;;        (recursive-query producer
;;          (create-pager producer (+ offset i) amount))
;;     :else (cons 
;;      (take 1 (:results pager))
;;      (lazy-seq (recursive-query
;;                  producer
;;                  pager
;;                  (dec remaining)
;;                  (inc i)
;;                  coll))))))
;;


(defn create-producer [tx reader query sort-field sort-order]
  (fn [offset amount]
    (convert-results-to-documents tx
      (drop offset (lucene/query reader query (+ offset amount) sort-field sort-order)))))

(defn create-pager [producer offset amount]
  {
   :offset offset
   :amount amount
   :results (producer offset amount)
   :more (fn [] (create-pager producer (+ offset amount) amount))
   })
  

(defn perform-query 
  [producer offset amount]
  (loop [results ()
         pager (create-pager producer offset amount)]
      (let [new-results (take amount (concat results (:results pager)))
            new-total (count new-results)]

           (if (and (= (count (:results pager)) 0)
                    (not= new-total amount))
             (recur new-results 
                    ((:more pager)))
             new-results))))

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


