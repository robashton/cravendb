(ns cravendb.connection
  (:require [cravendb.embeddeddatabase :as embedded]))

(defn creation-options [kvs]
  (merge { :href nil :path nil } (apply hash-map kvs)))

(defn create-embedded [opts] (embedded/create opts))

(defn create-remote [opts]
  { :href (:href opts)})

(defn create
  "Creates a database for use in the application
  Possible options are
    :path is where to store the data, this is for embedded databases
    :href is the location of the remote database
  if no :path or :href is specified, then it is assumed you want an in-memory database"
  [& kvs]
  (let [opts (creation-options kvs)]
    (if (:href opts) 
      (create-remote opts)
      (create-embedded opts))))
