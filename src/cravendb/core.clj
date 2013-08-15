(ns cravendb.core
  (:require [cravendb.leveldb :as leveldb]))

(def db (atom nil))

(defn open [dir]
  (reset! db (leveldb/open-db dir)))

(defn close []
  (leveldb/close-db @db)
  (reset! db nil))

(defn instance [] @db)

