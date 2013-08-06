(ns cravendb.core
  (:require [cravendb.documents :as docs]))

(def db (atom nil))

(defn open []
  (reset! db (docs/db "test")))

(defn close []
  (.close db)
  (reset! db nil))

(defn instance [] @db)

