(ns cravendb.core
  (:require [cravendb.documents :as docs]))

(def db (atom nil))

(defn open [dir]
  (reset! db (docs/db dir)))

(defn close []
  (.close @db)
  (reset! db nil))

(defn instance [] @db)

