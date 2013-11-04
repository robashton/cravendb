(ns cravendb.inflight
  (:require [cravendb.vclock :as v]
            [cravendb.documents :as docs]
            [clojure.core.incubator :refer [dissoc-in]]
            [cravendb.database :as db]
            [cravendb.storage :as s]))

;; This needs to work for
  ;; Single writes via database->
  ;; Multi writes via bulk->
  ;; Multi writes via replication

(defn create [db server-id]
  { :server-id server-id :db db :tx-count (atom 0) :in-flight (atom {})})
    
(defn open [{:keys [in-flight db tx-count]}]
  (let [txid (swap! tx-count inc)] 
    (swap! in-flight #(assoc-in %1 [:transactions txid] 
                      { :tx (s/ensure-transaction db)
                        :ops () }))
    txid))

(defn history-in-db [db id]
  (get (docs/load-document-metadata db id) :history))

(defn history-in-flight [in-flight id]
  (get-in in-flight [:documents id :history]))

(defn conflict-status 
  [current supplied]
  (cond
    (nil? current) :write ;; We don't know about this document
    (v/same? supplied current) :write ;; Client didn't specify and no conflict
    (v/descends? supplied current) :write ;; Client specified, knows the future 
    :else :conflict)) ;; Who knows, conflict!

(defn adjust-metadata 
  [tx metadata old-history]
  (assoc metadata 
         :synctag (s/next-synctag tx)
         :history (v/next (str ""))))

(defn add-success
  [in-flight txid id request document metadata]
  
  )

(defn add-conflict
  [in-flight txid id request document metadata]
  
  )

(defn write-request [db txid request id document metadata]
  (fn [in-flight]
    (-> in-flight
      (update-in [:transactions txid :ops] 
                conj { 
                  :id id
                  :request request 
                  :document document 
                  :metadata metadata})
      (assoc-in [:documents id] { :document document :metadata metadata}))))

(defn finish-with-document [in-flight doc-id]
  (dissoc-in in-flight [:documents doc-id]))

(defn clean-up [txid]
  (fn [{:keys [in-flight]}]
    (->(reduce finish-with-document 
            in-flight 
            (map :id (get-in in-flight [:transactions txid :ops])))
      (dissoc-in [:transactions txid]))))

(defn is-registered? 
  [{:keys [in-flight]} id]
  (boolean (get-in @in-flight [:documents id])))

(defn is-txid?
  [{:keys [in-flight]} txid]
  (boolean (get-in @in-flight [:transactions txid])))

(defn add-document [{:keys [in-flight db]} txid id document metadata]
  (swap! in-flight (write-request db txid :doc-add id document metadata)))

(defn remove-document [{:keys [in-flight db]} tx txid id metadata]
  (swap! in-flight (write-request db txid :doc-delete id nil metadata)))
 
(defn write-operation [tx {:keys [request id document metadata]}] 
  (case request
    :doc-add (docs/store-document tx id document metadata)))

;; For calling once a transaction is actually committed
(defn complete! [{:keys [in-flight]} txid]
  (let [{:keys [tx ops]} (get-in @in-flight [:transactions txid])] 
    (s/commit! (reduce write-operation tx ops)))
  (swap! in-flight (clean-up txid)))
