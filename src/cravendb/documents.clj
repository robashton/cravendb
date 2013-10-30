(ns cravendb.documents
  (:use [clojure.tools.logging :only (info error debug)] 
       [cravendb.core])
  (:require [cravendb.storage :as s]
            [clojure.edn :as edn]
            ))


(def synctags-to-docs-prefix "synctags-to-docs-")
(def conflict-prefix "conflict-")
(def document-prefix "doc-")
(def last-synctag-key "last-synctag")
(def docs-to-metadata-prefix "docs-to-metadata-")

(defn is-document-key [^String k]
  (.startsWith k document-prefix))

(defn is-document-key-prefixed-with [prefix entry]
  (.startsWith (entry :k) (str document-prefix prefix)))

(defn is-synctags-to-docs-key [k]
  (.startsWith k synctags-to-docs-prefix))

(defn is-synctag-docs-entry [m]
  (is-synctags-to-docs-key (:k m)))

(defn is-conflict-entry [m]
  (.startsWith (:k m) conflict-prefix))

(defn is-conflict-entry-for [m doc-id]
  (.startsWith (:k m) (str conflict-prefix doc-id)))


(defn last-synctag-in
  [storage]
  (or (s/get-string storage last-synctag-key) (zero-synctag)) )

(defn write-last-synctag
  [tx last-synctag]
  (s/store tx last-synctag-key (integer-to-synctag last-synctag)))

(defn store-conflict [db id document metadata]
  (s/store db (str conflict-prefix id (:synctag metadata))
           (pr-str {
                    :id id
                    :data document
                    :metadata metadata })))

(defn conflicts 
  ([db] (conflicts db ""))
  ([db prefix]
    (debug "About to iterate conflicts" prefix)
      (with-open [iter (s/get-iterator db )] 
        (s/seek iter (str conflict-prefix prefix))
        (doall (->> (s/as-seq iter) 
            (take-while #(is-conflict-entry-for %1 prefix))
            (map (comp edn/read-string :v)))))))

(defn without-conflict [tx doc-id synctag]
   (s/delete tx (str conflict-prefix doc-id synctag)))

(defn without-conflicts [tx doc-id]
  (reduce #(without-conflict %1 (:id %2) (get-in %2 [:metadata :synctag])) tx (conflicts tx doc-id)))

(defn store-document 
  [db id document metadata] 
  (-> db
    (s/store (str document-prefix id) (pr-str document))
    (s/store (str synctags-to-docs-prefix (:synctag metadata)) id)
    (s/store (str docs-to-metadata-prefix id) (pr-str metadata))))

(defn load-document [session id] 
  (if-let [raw-doc (s/get-string session (str document-prefix id))]
    (edn/read-string raw-doc) nil))

(defn load-document-metadata [session id]
  (if-let [raw-doc (s/get-string session (str docs-to-metadata-prefix id))]
    (edn/read-string raw-doc) nil))

(defn synctag-for-doc [db doc-id]
  (:synctag (load-document-metadata db doc-id)))

(defn delete-document 
  [session id metadata]
  (-> session
    (s/delete (str document-prefix id))
    (s/store (str synctags-to-docs-prefix (:synctag metadata)) id)
    (s/store (str docs-to-metadata-prefix id) (pr-str metadata))))

(defn iterate-documents-prefixed-with [iter prefix]
  (s/seek iter (str document-prefix prefix))
  (->> (s/as-seq iter)
    (take-while (partial is-document-key-prefixed-with prefix))
    (map (comp edn/read-string :v))) )

(defn iterate-synctags-after [iter synctag]
  (debug "About to iterate synctags after" synctag)
  (s/seek iter (str synctags-to-docs-prefix (next-synctag synctag)))
  (->> (s/as-seq iter) (take-while is-synctag-docs-entry) (map :v) (distinct)))


