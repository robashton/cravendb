(ns cravendb.documents
  (:use [clojure.tools.logging :only (info error debug)] 
       [cravendb.core])
  (:require [cravendb.storage :as s]
            [clojure.edn :as edn]
            ))

(def etags-to-docs-prefix "etags-to-docs-")
(def docs-to-etags-prefix "docs-to-etags-")
(def conflict-prefix "conflict-")
(def document-prefix "doc-")
(def last-etag-key "last-etag")

(defn is-document-key [^String k]
  (.startsWith k document-prefix))

(defn is-document-key-prefixed-with [prefix entry]
  (.startsWith (entry :k) (str document-prefix prefix)))

(defn is-etags-to-docs-key [k]
  (.startsWith k etags-to-docs-prefix))

(defn is-etag-docs-entry [m]
  (is-etags-to-docs-key (m :k)))

(defn is-conflict-entry [m]
  (.startsWith (m :k) conflict-prefix))

(defn is-conflict-entry-for [m doc-id]
  (.startsWith (m :k) (str conflict-prefix doc-id)))

(defn etag-for-doc [db doc-id]
  (s/get-string db (str docs-to-etags-prefix doc-id)))

(defn last-etag-in
  [storage]
  (or (s/get-string storage last-etag-key) (zero-etag)) )

(defn write-last-etag
  [tx last-etag]
  (s/store tx last-etag-key (integer-to-etag @last-etag)))

(defn store-conflict [db id document old-etag new-etag]
  (s/store db (str conflict-prefix id new-etag)
           (pr-str {
                    :etag new-etag
                    :id id
                    :data document })))



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

(defn without-conflict [tx doc-id etag]
   (s/delete tx (str conflict-prefix doc-id etag)))

(defn without-conflicts [tx doc-id]
  (reduce #(without-conflict %1 (:id %2) (:etag %2)) tx (conflicts tx doc-id)))

(defn store-document [db id document etag] 
  (-> db
    (s/store (str document-prefix id) (pr-str document))
    (s/store (str etags-to-docs-prefix etag) id)
    (s/store (str docs-to-etags-prefix id) etag)))

(defn load-document [session id] 
  (if-let [raw-doc (s/get-string session (str document-prefix id))]
    (edn/read-string raw-doc) nil))

(defn delete-document [session id etag]
  (-> session
    (s/delete (str document-prefix id)
    (s/store (str etags-to-docs-prefix etag) id)
    (s/store (str docs-to-etags-prefix id) etag))))

(defn iterate-documents-prefixed-with [iter prefix]
  (.seek iter (s/to-db (str document-prefix prefix)))
  (->> 
    (iterator-seq iter)
    (map expand-iterator-str)
    (take-while (partial is-document-key-prefixed-with prefix))
    (map (comp edn/read-string extract-value-from-expanded-iterator))) )

(defn iterate-etags-after [iter etag]
  (debug "About to iterate etags after" etag)
  (.seek iter (s/to-db (str etags-to-docs-prefix (next-etag etag))))
  (->> 
    (iterator-seq iter)
    (map expand-iterator-str)
    (take-while is-etag-docs-entry)
    (map extract-value-from-expanded-iterator)
    (distinct)))
