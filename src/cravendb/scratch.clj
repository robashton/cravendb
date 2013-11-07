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
           [cravendb.indexengine :as ie]
           [cravendb.defaultindexes :as di]
           [cravendb.indexing :as indexing]
           [cravendb.tasks :as tasks]
           [clojure.edn :as edn]))

(def current (atom nil))

(def test-index
  { :id "test-index" :map "(fn [doc] { \"foo\" (doc :foo) })"})

(def test-new-index
  { :id "test-new-index" :map "(fn [doc] { \"foo\" (doc :foo) })"})

(defn test-start [e]
  (let [db (s/create-in-memory-storage)
        engine (ie/create db)]
    (with-open [tx (s/ensure-transaction db)]
      (-> tx
        (indexes/put-index test-index (s/next-synctag tx))
        (s/commit!)))
    (ie/start engine)
    { :db db
      :engine engine}))

(defn test-stop [{:keys [db engine]}]
  (ie/stop engine)
  (.close db)
  nil)

(defn test-restart [e]
  (if e (test-stop e))
  (test-start e))

#_ (swap! current test-start)
#_ (swap! current test-stop)
#_ (swap! current test-restart)

#_ (with-open [tx (s/ensure-transaction db)]
      (-> tx
        (indexes/put-index test-new-index (s/next-synctag tx))
        (s/commit!)))

#_ (with-open [tx (s/ensure-transaction (:db @current))]
     (-> tx
       (docs/store-document "doc-1" { :foo "bar1" } { :synctag (s/next-synctag tx)})
       (docs/store-document "doc-2" { :foo "bar2" } { :synctag (s/next-synctag tx)})
       (docs/store-document "doc-3" { :foo "bar3" } { :synctag (s/next-synctag tx)})
       (docs/store-document "doc-4" { :foo "bar4" } { :synctag (s/next-synctag tx)})
       (docs/store-document "doc-5" { :foo "bar5" } { :synctag (s/next-synctag tx)})
       (s/commit!)))
#_ (with-open [tx (s/ensure-transaction (:db @current))]
     (-> tx
       (docs/store-document "doc-6" { :foo "bar6" } { :synctag (s/next-synctag tx)})
       (docs/store-document "doc-7" { :foo "bar7" } { :synctag (s/next-synctag tx)})
       (docs/store-document "doc-8" { :foo "bar8" } { :synctag (s/next-synctag tx)})
       (docs/store-document "doc-9" { :foo "bar9" } { :synctag (s/next-synctag tx)})
       (docs/store-document "doc-10" { :foo "bar10" } { :synctag (s/next-synctag tx)})
       (s/commit!)))

#_ (indexes/get-last-indexed-synctag-for-index (:db @current) (:id test-index))

;; If we start up a database with an index, and add documents we should be able to query them
;; If we start up a database, add documents, then a new index, we should be able to query it
;; That it is all we expect. Everything else is an optimisation
