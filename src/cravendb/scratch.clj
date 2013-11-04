(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.data.codec.base64])
  (:require [cravendb.vclock :as v]
            [cravendb.documents :as docs]
            [cravendb.tasks :as tasks]
            [cravendb.http :as http]
            [clojurewerkz.vclock.core :as vclock]            
            [org.httpkit.server :refer [run-server]]
            [clojure.edn :as edn]
            [cravendb.database :as db]
            [cravendb.storage :as s]
            [me.raynes.fs :as fs]
            [cravendb.client :as client]
            [cravendb.replication :as r]
            [clojure.pprint :refer [pprint]]))


(defn start []
  (def source (db/create "testdb_source" :server-id "src")))

(defn stop []
  (.close source) 
  (fs/delete-dir "testdb_source"))

(defn restart []
  (stop)
  (start))

#_ (start)
#_ (stop)
#_ (restart)


;; This needs to work for
  ;; Single writes via database->
  ;; Multi writes via bulk->
  ;; Multi writes via replication

(defn create [db]
  { :db db :tx-count (atom 0) :in-flight (atom {})})
    
(defn open [{:keys [in-flight db tx-count]}]
  (let [txid (swap! tx-count inc)] 
    (swap! in-flight #(assoc-in %1 [:transactions txid] 
                      { :tx (s/ensure-transaction db)}))
    txid))

(defn write-request [txid id document metadata]
  (fn [in]
    ;; Should get the history from the tx
    ;; Should compare that against what we have in memory
    ;; Should set the known history for that id in memory
    ;; Should increase the counter for that known history
    ;; Should map this id to the transaction id so we know to remove when done
    ))

(defn clean-up [txid]
  (fn [in]
    ;; Should decrease the counter for any ids held by this transaction
    ;; Should remove this transaction
    ;; Should remove any ids that have a counter of 0
    ))

(defn add-document [{:keys [in-flight]} txid id document metadata]
  (swap! in-flight (write-request txid :doc-add id document metadata)))

(defn remove-document [{:keys [in-flight]} tx txid id metadata]
  (swap! in-flight (write-request txid :doc-delete id nil metadata)))

;; For calling once a transaction is actually committed
(defn complete! [{:keys [in-flight]} txid]
  ;; de-ref that specific id
  ;; commit the transaction
  ;; remove it from the in-flight register
  (swap! in-flight (clean-up txid)))

