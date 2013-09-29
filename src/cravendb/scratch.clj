(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.client :as client]
            [cravendb.query :as query]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as storage]
            [me.raynes.fs :as fs]
            [ring.adapter.jetty :refer [run-jetty]]
            [cravendb.http :as http]  
            [cravendb.lucene :as lucene])
  (use [cravendb.testing]
       [cravendb.core]
       [clojure.tools.logging :only (info debug error)] 
       [clojure.pprint]))

;;(declare document-description)
;;(declare get-type-description)
;;
;;(defn get-type-description [v]
;;  (if (string? v) :string
;;    (if (integer? v) :integer
;;      (if (list v) :list) 
;;        (if (map? v) 
;;        (for [[k sv] v] {  
;;          k (get-type-description sv) })
;;        ))))
;;
;;(get-type-description "hello")
;;(get-type-description 1337)
;;(get-type-description ())
;;(get-type-description [])
;;(get-type-description {})
;;
;;(get-type-description { :title "hello" :age 27 :children [] })
;;

(defn generate-key-name [prefix k]
  (clojure.string/replace (str (if prefix (str prefix "$")) k) ":" ""))

(defn flatten-document 
  ([doc prefix]
    (into {} (for [[k v] doc]
      [
       (generate-key-name prefix k) 
       (if (map? v) (flatten-document v (generate-key-name prefix k))
          v) ])))
  ([doc] (flatten-document doc nil)))

;; Array of strings should be indexed as array: [ "value" "value" "value" ]
;; Array of objects should be indexed as multiple arrays 
;; children$name [ "billy" "sally" ]
;; children$gender ["male" "female" ]
;; Note: This raises the usual issue of "how do I find people with children called billy who are male"
;; Answer: Write your own sodding index "billy_male" and search for that.

#_ (flatten-document {
                      :title "hello" 
                      :age 27 
                      :address 
                        { 
                         :line-one "3 Ridgeborough Hill" 
                         :post-code "IM4 7AS"}
                      :pets [ "bob" "harry" "dick"]
                      :children [
                        { :name "billy" :gender "male" }
                        { :name "sally" :gender "female" }
                      ] 
                      })
