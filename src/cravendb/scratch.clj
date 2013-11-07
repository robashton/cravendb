(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.tools.logging :only (info debug error)]
        [clojure.data.codec.base64]
        [clojure.core.async])
  (:import (java.io File File PushbackReader IOException FileNotFoundException ))
  (:require [cravendb.lucene :as lucene]
           [cravendb.storage :as s]
           [cravendb.documents :as docs]
           [clojure.core.incubator :refer [dissoc-in]]
           [me.raynes.fs :as fs]
           [cravendb.indexstore :as indexes]
           [cravendb.database :as db]
           [cravendb.indexengine :as ie]
           [cravendb.indexing :as indexing]
           [cravendb.defaultindexes :as di]
           [cravendb.indexing :as indexing]
           [cravendb.tasks :as tasks]
           [clojure.edn :as edn]))


(def test-index
  { :id "test-index" :map "(fn [doc] { \"foo\" (doc :foo) })"})

(def test-new-index
  { :id "test-new-index" :map "(fn [doc] { \"foo\" (doc :foo) })"})

(defn test-start [e]
  (let [{:keys [storage] :as instance} (db/create)]
    (db/put-index instance test-index)
    instance))

(defn test-stop [instance]
  (.close instance))

(defn test-restart [e]
  (if e (test-stop e))
  (test-start e))

#_ (swap! current test-start)
#_ (swap! current test-stop)
#_ (swap! current test-restart)


#_ (do 
    (db/put-document @current "doc-1" { :foo "bar1" })
    (db/put-document @current "doc-2" { :foo "bar2" }) 
    (db/put-document @current "doc-3" { :foo "bar3" }) 
    (db/put-document @current "doc-4" { :foo "bar4" }) 
    (db/put-document @current "doc-5" { :foo "bar5" })) 

#_ (db/put-index @current test-new-index)
#_ (indexing/wait-for-index-catch-up (:storage @current) 1)
#_ (indexes/get-last-indexed-synctag-for-index (:storage @current) (:id test-index))
#_ (indexes/get-last-indexed-synctag-for-index (:storage @current) (:id test-new-index))
#_ (s/last-synctag-in (:storage @current))

;; If we start up a database with an index, and add documents then it should be caught up
;; If we start up a database, add documents, then a new index, it should be caught up
;; If we start up a database, add documents, new index, new documents, all queries = valid
;; That it is all we expect. Everything else is an optimisation


