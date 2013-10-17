(ns cravendb.stream
  (:require [cravendb.documents :as docs]
            [cravendb.storage :as s]
            [cravendb.database :as db]
            [cravendb.core :refer [zero-etag]]))

(defn expand-document [instance id]
  {
   :doc (db/load-document instance id)
   :id id
   :metadata (db/load-document-metadata instance id)
   }) 

(defn from-etag [instance etag]
  (with-open [iter (s/get-iterator (:storage instance))] 
    (doall
      (map (partial expand-document instance) 
        (take 1024 (docs/iterate-etags-after iter etag))))))
