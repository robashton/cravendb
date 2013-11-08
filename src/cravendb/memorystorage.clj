(ns cravendb.memorystorage
  (:use [cravendb.core])
  (:require [cravendb.storageops :refer [Reader Writer Iterator Storage]]
            [clojure.tools.logging :refer [info debug error]]))

(def last-synctag-key "__last-synctag")

(defrecord MemoryIterator [snapshot memory start]
  java.io.Closeable
  Iterator
  (seek! [this value] (swap! start (fn [i] value)))
  (as-seq [this]
    (map (fn [i] {:k (key i) :v (val i)}) 
      (drop-while #(> 0 (compare (key %1) @start)) (or snapshot @memory))))
  (close [this] nil))

(defrecord MemoryTransaction [snapshot memory]
  java.io.Closeable
  Writer
  Reader
  (open-iterator [this] (MemoryIterator. snapshot memory (atom nil)))
  (from-db [this id] (get snapshot id))
  (commit! [this] (swap! memory #(reduce (fn [m [k v]] 
                  (if (= :deleted v) (dissoc m k) (assoc m k v))) 
                                        %1 (:cache this))))
  (close [this] nil))

(defrecord MemoryStorage [memory]
  java.io.Closeable
  Reader
  Storage
  (ensure-transaction [ops] (MemoryTransaction. @(:memory ops) (:memory ops)))
  (from-db [this id] (get @memory id))
  (open-iterator [this] (MemoryIterator. nil memory (atom nil)))
  (close [this] nil)) 

(defn create [] (MemoryStorage. (atom (sorted-map))))
