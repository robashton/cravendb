(ns cravendb.documents
  (:use [clojure.tools.logging :only (info error debug)] 
       [cravendb.core])
  (:require [cravendb.storage :as s]
            [clojure.edn :as edn]
            ))


(def synctags-to-docs-prefix "synctags-to-docs-")
(def docs-to-synctags-prefix "docs-to-synctags-")
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
  (is-synctags-to-docs-key (m :k)))

(defn is-conflict-entry [m]
  (.startsWith (m :k) conflict-prefix))

(defn is-conflict-entry-for [m doc-id]
  (.startsWith (m :k) (str conflict-prefix doc-id)))

(defn synctag-for-doc [db doc-id]
  (s/get-string db (str docs-to-synctags-prefix doc-id)))

(defn last-synctag-in
  [storage]
  (or (s/get-string storage last-synctag-key) (zero-synctag)) )

(defn write-last-synctag
  [tx last-synctag]
  (s/store tx last-synctag-key (integer-to-synctag last-synctag)))

(defn store-conflict [db id document synctag metadata]
  (s/store db (str conflict-prefix id synctag)
           (pr-str {
                    :synctag synctag
                    :id id
                    :data document
                    :metadata metadata })))

(defn conflicts 
  ([db] (conflicts db ""))
  ([db prefix]
    (debug "About to iterate conflicts" prefix)
      (with-open [iter (s/get-iterator db)] 
        (.seek iter (s/to-db (str conflict-prefix prefix)))
        (doall
          (->> 
            (iterator-seq iter)
            (map expand-iterator-str)
            (take-while #(is-conflict-entry-for %1 prefix))
            (map (comp edn/read-string extract-value-from-expanded-iterator)))))))

(defn without-conflict [tx doc-id synctag]
   (s/delete tx (str conflict-prefix doc-id synctag)))

(defn without-conflicts [tx doc-id]
  (reduce #(without-conflict %1 (:id %2) (:synctag %2)) tx (conflicts tx doc-id)))

(defn store-document 
  ([db id document synctag] (store-document db id document synctag {}))
  ([db id document synctag metadata] 
  (-> db
    (s/store (str document-prefix id) (pr-str document))
    (s/store (str synctags-to-docs-prefix synctag) id)
    (s/store (str docs-to-synctags-prefix id) synctag)
    (s/store (str docs-to-metadata-prefix id) (pr-str metadata)))))

(defn load-document [session id] 
  (if-let [raw-doc (s/get-string session (str document-prefix id))]
    (edn/read-string raw-doc) nil))

(defn load-document-metadata [session id]
  (if-let [raw-doc (s/get-string session (str docs-to-metadata-prefix id))]
    (edn/read-string raw-doc) nil))

(defn delete-document 
  ([session id synctag] (delete-document session id synctag {}))
  ([session id synctag metadata]
  (-> session
    (s/delete (str document-prefix id))
    (s/store (str synctags-to-docs-prefix synctag) id)
    (s/store (str docs-to-synctags-prefix id) synctag)
    (s/store (str docs-to-metadata-prefix id) (pr-str metadata)))))

(defn iterate-documents-prefixed-with [iter prefix]
  (.seek iter (s/to-db (str document-prefix prefix)))
  (->> 
    (iterator-seq iter)
    (map expand-iterator-str)
    (take-while (partial is-document-key-prefixed-with prefix))
    (map (comp edn/read-string extract-value-from-expanded-iterator))) )

(defn iterate-synctags-after [iter synctag]
  (debug "About to iterate synctags after" synctag)
  (.seek iter (s/to-db (str synctags-to-docs-prefix (next-synctag synctag))))
  (->> 
    (iterator-seq iter)
    (map expand-iterator-str)
    (take-while is-synctag-docs-entry)
    (map extract-value-from-expanded-iterator)
    (distinct)))
