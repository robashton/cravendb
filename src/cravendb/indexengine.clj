(ns cravendb.indexengine
  (:use [cravendb.core]
       [clojure.pprint]
       [clojure.tools.logging :only (info debug error)])
  (:import (java.io File File PushbackReader IOException FileNotFoundException ))
  (:require [cravendb.lucene :as lucene]
           [cravendb.storage :as s]
           [cravendb.indexstore :as indexes]
           [cravendb.defaultindexes :as di]
           [cravendb.indexing :as indexing]))

(defn storage-path-for-index [index]
  (str (:id index) "-" (or (:etag index) "")))

(defn open-storage-for-index [path index]
  (let [storage (lucene/create-index (File. path (storage-path-for-index index)))]
    (-> index
      (assoc :storage storage)
      (assoc :writer (lucene/open-writer storage)))))

(defn read-index-data [tx index-string]
  (let [index (read-string index-string)]
    (assoc index :etag (indexes/etag-for-index tx (:id index)))))

(defn all-indexes [db]
  (with-open [tx (s/ensure-transaction db)
              iter (s/get-iterator tx)]
    (doall (map (partial read-index-data tx) (indexes/iterate-indexes iter)))))

(defn ex-error [prefix ex]
  (error prefix (ex-expand ex)))

(defn compile-index [index]
  (assoc index 
         :map (load-string (index :map))
         :filter (if (:filter index) (load-string (:filter index)) nil)))

(defn map-indexes-by-id [indexes]
 (into {} (for [i indexes] [(:id i) i])))

(defn load-initial-indexes [db]
  (map-indexes-by-id 
    (map (partial open-storage-for-index (:path db))  
       (concat (di/all) (map compile-index (all-indexes db))))))

(defn close-engine [engine]
  (debug "Closing the engine")
  (doseq [[id i] (:compiled-indexes engine)] 
    (do
      (.close (i :writer)) 
      (.close (i :storage)))))

(defn compiled-indexes [handle] 
  (map val (:compiled-indexes @(:ea handle))))

(defn prepare-indexes [indexes db]
  (map (comp (partial open-storage-for-index (:path db)) compile-index) indexes))

(defn index-is-equal [index-one index-two]
  (and (= (:id index-one) (:id index-two))
       (= (:etag index-one) (:etag index-two))))

(defn new-indexes [db current all]
  (map-indexes-by-id
    (prepare-indexes 
      (filter #(not-any? (partial index-is-equal %1) 
        (map val current)) all) db)))

(defn deleted-indexes [current all]
  (into {} 
    (for [i (filter #(not-any? (partial = %1) (map :id all))
      (map :id (filter :etag (map val current))))] [i nil])))

(defn close-obsolete-indexes! [existing new deleted]
  (doseq [[id index] deleted]
    (if-let [current (existing id)]
      (do 
          (info "deletion detected, closing writers for" id)
          (.close (:writer current))
          (.close (:storage current)))))
  (doseq [[id index] new]
     (if-let [current (existing id)]
      (do 
          (info "overwrite detected, closing writers for" id)
          (.close (:writer current))
          (.close (:storage current))))))

(defn refresh-indexes! [{:keys [db compiled-indexes] :as engine}]
  (let [all (all-indexes #spy/p db)
        newly-opened (new-indexes db compiled-indexes all)
        newly-deleted (deleted-indexes compiled-indexes all)] 
    (close-obsolete-indexes! compiled-indexes newly-opened newly-deleted)
    (assoc engine :compiled-indexes
      (-> (apply dissoc compiled-indexes (map key newly-deleted))
          (merge newly-opened)))))

(defn remove-any-finished-chasers [engine]
  (debug "Removing chasers that aren't needed")
  (assoc engine :chasers
        (filter #(not (realized? (:future %1))) (:chasers engine))))

(defn needs-a-new-chaser [engine index]
  (debug "Checking if we need a new chaser for" (:id index))
  (and
    (not= 
      (indexing/last-indexed-etag (:db engine)) 
      (indexes/get-last-indexed-etag-for-index 
        (:db engine) 
        (:id index)))
    (not-any? 
      (partial = (:id index))
      (map :id (:chasers engine)))))

(defn create-chaser [engine index]
  (info "Starting a freaking chaser for " (:id index))
  {
   :id (:id index)
   :future 
    (future
      (indexing/index-catchup! (:db engine) index))
   })

(defn indexes-which-require-a-chaser [engine]
  (filter 
    #(needs-a-new-chaser engine %1) 
    (map val (:compiled-indexes engine))))

(defn start-new-chasers [engine]
  (debug "Starting new chasers")
  (assoc engine :chasers
    (doall
      (concat 
      (:chasers engine)
      (doall 
        (map #(create-chaser engine %1) 
          (indexes-which-require-a-chaser engine)))))))

(defn indexes-which-are-up-to-date [engine]
  (filter #(not-any? 
             (partial = (:id %1)) 
             (map :id (:chasers engine))) 
          (map val (:compiled-indexes engine))))

(defn pump-indexes-at-head! [engine]
  (indexing/index-documents! 
    (:db engine) 
    (indexes-which-are-up-to-date engine)))

(defn mark-pump-as-complete [engine]
  (debug "Index chaser complete")
  (assoc engine :running-pump false))

(defn pump-indexes! [engine]
  (-> engine 
    remove-any-finished-chasers 
    start-new-chasers
    pump-indexes-at-head!
    mark-pump-as-complete))

(defn try-pump-indexes [engine ea]
 (if (:running-pump engine) 
   engine
   (do
     (send ea pump-indexes!)
     (assoc engine :running-pump true))))

(defn start-indexing [engine ea]
  (let [task (future 
        (loop []
          (send ea refresh-indexes!)
          (send ea try-pump-indexes ea)
          (Thread/sleep 50)
          (recur)))]
   (assoc engine :worker-future task)))

(defn stop-indexing [engine]
  (future-cancel (:worker-future engine))
  (assoc engine :worker-future nil))

(defprotocol EngineOperations
  (close [this]))

(defrecord EngineHandle [ea]
  EngineOperations
  (close [this]
    (debug "Closing engine handle")
    (send ea close-engine)
    (await ea)))

(defn start [ops]
  (send (:ea ops) start-indexing (:ea ops)))

(defn get-index-storage [ops index-id]
  (get-in @(:ea ops) [:compiled-indexes index-id :storage]))

(defn stop [ops]
 (debug "Stopping indexing agents")
 (send (:ea ops) stop-indexing)
 (await (:ea ops)))

(defn handle-agent-error [engine e]
  (ex-error "Indexing engine fell over" e))

(defn create-engine [db]
  (let [engine 
        (agent {
          :chasers ()
          :db db
          :compiled-indexes (load-initial-indexes db) })]
    (set-error-handler! engine handle-agent-error)
    (EngineHandle. engine))) 
