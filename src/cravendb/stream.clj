(ns cravendb.stream
  (:require [cravendb.documents :as docs]
            [cravendb.storage :as s]
            [cravendb.database :as db]
            [cravendb.core :refer [zero-synctag]]))

(defn expand-document [instance id]
  {
   :doc (db/load-document instance id)
   :id id
   :metadata (db/load-document-metadata instance id)
   }) 

(defn from-synctag [instance synctag]
  (with-open [iter (s/get-iterator (:storage instance))] 
    (doall
      (map (partial expand-document instance) 
        (take 1024 (docs/iterate-synctags-after iter synctag))))))
