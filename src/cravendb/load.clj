(ns cravendb.load
  (:use [cravendb.core]
       [clojure.tools.logging :only (info error debug)] )
   (:require [clojure.data.csv :as csv]
            [me.raynes.fs :as fs]
            [clojure.java.io :as io]
            [clojure.string :refer [trim]]
            [cravendb.http :as http]
            [cravendb.transaction :as t]
            [cravendb.remote :as remote]
            [cravendb.storage :as storage]
            [cravendb.documents :as docs]
            [cravendb.query :as query]
            [cravendb.indexengine :as indexengine]
             ))

(defn read-float [in i]
  (Float/parseFloat (get in i)))

(defn read-int [in i]
  (int (read-float in i)))

(defn read-str [in i]
  (trim (get in i)))
(defn prescription-row [in]
  {
   :sha  (read-str in 0)
   :pct (read-str in 1)
   :practice (read-str in 2)
   :bnf-code (read-str in 3)
   :bnf-chemical (read-str in 4)
   :bnf-name (read-str in 5)
   :items (read-float in 6)
   :net-ingredient-cost (read-float in 7)
   :act-cost (read-float in 8)
   :quantity (read-int in 9)
   :year (read-int in 10)
   :month (read-int in 11)
   }
)

(defn gp-row [in]
  {
    :organisation-code (read-str in 0) 
    :name (read-str in 1) 
    :national-grouping (read-str in 2) 
    :high-level-health-authority (read-str in 3) 
    :address-1 (read-str in 4) 
    :address-2 (read-str in 5) 
    :address-3 (read-str in 6) 
    :address-4 (read-str in 7) 
    :address-5 (read-str in 8) 
    :postcode (read-str in 9) 
    :open-date (read-str in 10) 
    :close-date (read-str in 11) 
    :status-code (read-str in 12) 
    :org-sub-type-code (read-str in 13) 
    :parent-org-code (read-str in 14) 
    :join-parent-date (read-str in 15) 
    :left-parent-date (read-str in 16) 
    :contact-telephone (read-str in 17) 
    :amend-record-indicator (read-str in 18) 
    :practice-type (read-str in 19) 
   }
  )

(defn add-sequential-doc-to-transaction [{:keys [tx instance prefix id total] :as state} item]
  (if (= 0 (mod total 250))
    (do
      (info "Flushing after" total)
      (t/commit! tx)
      (-> state
        (assoc :tx (t/store (t/open instance) (str prefix "-" id) item))       
        (assoc :id (inc id)) 
        (assoc :total (inc total))))
    (do
      (-> state
        (assoc :tx (t/store tx (str prefix "-" id) item))       
        (assoc :id (inc id)) 
        (assoc :total (inc total))))))

;(defn import-prescriptions []
;  (time (with-open [in-file (io/reader "input/prescriptions/adhd/part-00000")]
;     (t/commit! (:tx (reduce add-sequential-doc-to-transaction {
;        :tx (t/open "http://localhost:9002")
;        :id 0
;        :total 0
;        :prefix "scrips"
;       }
;      (map prescription-row (csv/read-csv in-file))))))))
;
#_ (time (with-open [in-file (io/reader "input/epraccur.csv")
                     instance (remote/create :href "http://localhost:8001") ]
     (t/commit!
       (:tx (reduce add-sequential-doc-to-transaction {
          :tx (t/open instance) 
          :id 0
          :instance instance
          :total 0
          :prefix "gp"
      }
                    (map gp-row (csv/read-csv in-file)))))))

