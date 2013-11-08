(ns cravendb.storageops)

(defprotocol Reader 
  (open-iterator [this])
  (from-db [this id]))

(defprotocol Writer
  (commit! [this]))

(defprotocol Iterator
  (seek! [this k])
  (as-seq [this]))

(defprotocol Storage
  (ensure-transaction [this]))


