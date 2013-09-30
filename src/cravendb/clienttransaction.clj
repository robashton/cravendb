(ns cravendb.clienttransaction
   (require [cravendb.client :as client]))

(defn store [tx id data]
  (assoc-in tx [:cache id] data))

(defn delete-document [tx id]
  (assoc-in tx [:cache id] :deleted))

(defn load-document [{:keys [href] :as tx} id]
  (let [cached (get-in tx [:cache id])]
    (if (= cached :deleted) nil
      (or cached (client/get-document href id)))))

(defn commit! [{:keys [href] :as tx}]
  (client/bulk-operation href
    (into () 
    (for [[k v] (:cache tx)]
      (if (= v :deleted)
        { :operation :docs-delete :id k }
        { :operation :docs-put :id k :document v })))))

(defn start [href] ({ :href href}))
