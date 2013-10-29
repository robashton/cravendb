(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.data.codec.base64])
  (:require [cravendb.vclock :as v]
            [clojure.pprint :refer [pprint]]
            [cravendb.documents :as docs]
            [clojurewerkz.vclock.core :as vclock]            
            [clojure.edn :as edn]
            [cravendb.database :as db]
            [cravendb.storage :as s]
            [me.raynes.fs :as fs]
            [cravendb.replication :as r]
            [cravendb.stream :as stream]
            [cravendb.client :as client]))

(defn start []
  (def instance (db/create "testdb")))

(defn stop []
  (.close instance))

(defn restart []
  (stop)
  (fs/delete-dir "testdb")
  (start))

#_ (start)
#_ (stop)
#_ (restart)

#_ (do
     (db/put-document instance "doc-1" { :foo "bar"})
     (db/put-document instance "doc-2" { :foo "bar"})
     (db/put-document instance "doc-3" { :foo "bar"})
     (db/put-document instance "doc-4" { :foo "bar"})
     (db/put-document instance "doc-5" { :foo "bar"})
     )

#_ (stream/from-synctag instance (zero-synctag))

#_ (def tx (assoc (s/ensure-transaction (:storage instance)) :e-id "1"
                  :base-vclock (:base-vclock instance)
                  :server-id "bob"))

#_ (db/check-document-write tx "doc-1"  {}
                            (fn [metadata]
                              (println "success" metadata))
                            (fn [metadata] 
                              (println "failure"  metadata)))

#_ (pprint (r/replicate-into tx
                       [
                        { :id "1" :doc {:foo "bar"} :metadata {:history "123235" :primary-server "bob"}}
                        { :id "2" :doc {:foo "bar"} :metadata {:history "123235" :primary-server "bob"}}
                        { :id "3" :doc {:foo "bar"} :metadata {:history "123235" :primary-server "bob"}}
                        { :id "4" :doc {:foo "bar"} :metadata {:history "123235" :primary-server "bob"}}
                        ]
                       ))

;; Replication doesn't currently check history
;; Replication doesn't currently check primary-server to prevent infinite replication
;; Replication doesn't currently create a new local synctag



;; Once we have this, we need to do it as a bulk operation via the database code-path
;; We *need* that in-flight transaction manager because bulk operations are *dangerous*
;; Y'know, it'd be great if synctags were a part of metadata








