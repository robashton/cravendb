(ns cravendb.clienttransaction
   (require [cravendb.client :as client]))


(defprotocol DocumentOperations
  (store-document [this id data])
  (load-document [this id])
  (delete-document [this id])
  (commit! [this]))

(defrecord HttpTransaction [href]
  DocumentOperations
  (store-document [this id data]
    (assoc-in this [:cache id] data))
  (delete-document [this id]
    (assoc-in this [:cache id] :deleted))
  (load-document [this id]
    (let [cached (get-in this [:cache id])]
      (if (= cached :deleted) nil
        (or cached (client/get-document href id)))))
  (commit! [this]
    (client/bulk-operation href
      (into () 
      (for [[k v] (:cache this)]
        (if (= v :deleted)
          { :operation :docs-delete :id k }
          { :operation :docs-put :id k :document v }))))))

(defn start [href] (HttpTransaction. href))
