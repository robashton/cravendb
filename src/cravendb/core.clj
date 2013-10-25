(ns cravendb.core
  (:require [cravendb.storage :as s]))


(defn integer-to-synctag [integer]
  (format "%030d" integer))

(defn synctag-to-integer [synctag]
  (Integer/parseInt synctag))

(defn zero-synctag [] (integer-to-synctag 0))

(defn newest-synctag [one two]
  (integer-to-synctag (max (synctag-to-integer one) (synctag-to-integer two))))

(defn next-synctag [synctag]
  (integer-to-synctag (inc (synctag-to-integer synctag))))

(defn ex-expand [ex]
  [ (.getMessage ex) (map #(.toString %1) (.getStackTrace ex))])

