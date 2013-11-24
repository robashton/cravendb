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

(defn valid-documents [tx results]
  (filter boolean (map (partial docs/load-document tx) results)))

(defn lucene-producer [tx reader opts]
  (fn [offset amount]
    (->> 
      (lucene/query reader 
                    (:filter opts) 
                    (+ offset amount) 
                    (:sort-by opts) 
                    (:sort-order opts)) 
      (drop offset) 
      (valid-documents tx))))

(defn lucene-page 
  ([producer page-size] (lucene-page producer 0 page-size))
  ([producer current-offset page-size]
   {
    :results (producer current-offset page-size)
    :next (fn [] (lucene-page producer (+ current-offset page-size) page-size))
   }))

(defn lucene-seq 
  ([page] (lucene-seq page (:results page)))
  ([page src]
   (cond
     (empty? (:results page)) ()
     (empty? src) (lucene-seq ((:next page)))
     :else (cons (first src) (lazy-seq (lucene-seq page (rest src)))))))

(defn query-with-storage [db storage {:keys [offset amount] :as opts}]
  (with-open [reader (lucene/open-reader storage)
              tx (s/ensure-transaction db)]
    (->>
      (lucene-page (lucene-producer tx reader opts) (+ offset amount)) 
      (lucene-seq)
      (drop offset)
      (take amount)
      (doall))))

(defn execute [db index-engine {:keys [index wait wait-duration] :as opts}]
  (if wait 
    (indexing/wait-for-index-catch-up db index wait-duration))
  (let [storage (indexengine/get-index-storage index-engine index)]
    (if storage 
      (do (try (query-with-storage db storage opts)
        (catch org.apache.lucene.index.IndexNotFoundException e nil)))
      (execute db index-engine (assoc opts :wait true :wait-duration 1)))))
