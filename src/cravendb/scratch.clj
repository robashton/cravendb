(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.data.codec.base64])
  (:require [cravendb.vclock :as v]
            [cravendb.documents :as docs]
            [cravendb.tasks :as tasks]
            [cravendb.indexing :as indexing]
            [cravendb.indexstore :as indexes]
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

(def write-three-documents 
  (fn [instance]
    (db/put-document instance "doc-1" (pr-str { :title "hello" :author "rob"}))
    (db/put-document instance "doc-2" (pr-str { :title "morning" :author "vicky"}))
    (db/put-document instance "doc-3" (pr-str { :title "goodbye" :author "james"}))))

(defn create-invalid-index []  
  {
   :id "invalid" 
   :map "(fn [doc] {\"hello\" ((:blah doc) :foo)})"
   })

(defn create-valid-index []  
  {
   :id "valid" 
   :map "(fn [doc] {\"hello\" (:author doc)})"
})

(defn -main []
  (println "Starting")
  (loop [] 
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (write-three-documents instance)
      (db/put-index instance (create-invalid-index))
      (indexing/wait-for-index-catch-up storage)
      (indexes/is-failed storage "invalid")))

    (with-full-setup (fn [{:keys [storage] :as instance}]
      (write-three-documents instance)
      (db/put-index instance (create-invalid-index))
      (indexing/wait-for-index-catch-up storage)
      (s/last-synctag-in storage) 
      (indexing/last-indexed-synctag storage))) 

    (with-full-setup (fn [{:keys [storage] :as instance}]
      (write-three-documents instance)
      (db/put-index instance (create-invalid-index))
      (db/put-index instance (create-valid-index))
      (indexing/wait-for-index-catch-up storage)
      (write-three-documents instance)
      (indexing/wait-for-index-catch-up storage)
      (s/last-synctag-in storage) 
      (indexing/last-indexed-synctag storage))) 

    (with-full-setup (fn [{:keys [storage] :as instance}]
      (write-three-documents instance)
      (db/put-index instance (create-invalid-index))
      (indexing/wait-for-index-catch-up storage)
      (with-open [tx (s/ensure-transaction storage)]
        (s/commit! (indexes/reset-index tx "invalid")))
      (zero-synctag) (indexes/get-last-indexed-synctag-for-index storage "invalid"))) 

    (with-full-setup (fn [{:keys [storage] :as instance}]
      (write-three-documents instance)
      (db/put-index instance (create-invalid-index))
      (indexing/wait-for-index-catch-up storage)
      (with-open [tx (s/ensure-transaction storage)]
        (s/commit! (indexes/reset-index tx "invalid")))
      (indexes/is-failed storage "invalid"))) 

    (recur)))
