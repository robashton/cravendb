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
                        :ops {} }))
    txid))

(defn history-in-db [db id]
  (get (docs/load-document-metadata db id) :history))

(defn history-in-flight [in-flight id]
  (get-in in-flight [:documents id :history]))

(defn conflict-status 
  [current supplied]
  (cond
    (nil? supplied) :write ;; They didn't bother giving us a history, last write wins
    (nil? current) :write ;; We don't know about this document so just write it
    (v/same? supplied current) :write ;; Isn't this nice, good little client
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

(defn winner-for [newtx docid]
  (fn [in-flight txid]
    (if (= txid newtx) in-flight
      (do
        (assoc-in 
          in-flight
          [:transactions newtx :ops docid :status] :conflict)))))

(defn post-write-checks
  [in-flight txid doc-id]
  (reduce 
    (winner-for txid doc-id) 
    in-flight 
    (get-in in-flight [:documents doc-id :refs])))

(defn write-request [db txid {:keys [metadata id] :as item}]
  (fn [in-flight]
    (-> in-flight
      (assoc-in [:transactions txid :ops id] item)
      (update-in [:documents id :refs] conj txid)
      (post-write-checks txid id))))

(defn finish-with-document [txid]
  (fn [in-flight doc-id] (dissoc-in in-flight [:documents doc-id])))

(defn clean-up [txid]
  (fn [in-flight]
    (-> (reduce (finish-with-document txid) 
            in-flight 
            (map (comp :id val) (get-in in-flight [:transactions txid :ops])))
      (dissoc-in [:transactions txid]))))

(defn is-registered? 
  [{:keys [in-flight]} id]
  (boolean (get-in @in-flight [:documents id])))

(defn is-txid?
  [{:keys [in-flight]} txid]
  (boolean (get-in @in-flight [:transactions txid])))

(defn add-document [{:keys [in-flight db]} txid id document metadata]
  (swap! in-flight (write-request db txid {:request :doc-add 
                                           :id id 
                                           :document document 
                                           :metadata metadata })))

(defn delete-document [{:keys [in-flight db]} txid id metadata]
  (swap! in-flight (write-request db txid {:request :doc-delete
                                           :id id 
                                           :metadata metadata })))
 
(defn write-operation [tx {:keys [status request id document metadata]}] 
  (case [status request]
    [nil :doc-add] (docs/store-document tx id document metadata)
    [nil :doc-delete] (docs/delete-document tx id metadata)
    [:conflict :doc-add] (docs/store-conflict tx id document metadata)
    [:conflict :doc-delete] (docs/store-conflict tx id :deleted metadata)))

;; For calling once a transaction is actually committed
(defn complete! [{:keys [in-flight]} txid]
  (let [{:keys [tx ops]} (get-in @in-flight [:transactions txid])] 
    (s/commit! (reduce write-operation tx (map val ops))))
  (swap! in-flight (clean-up txid)))
