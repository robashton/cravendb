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
  (:use [cravendb.testing]
       [cravendb.core]
       [clojure.tools.logging :only (info debug error)] 
       [clojure.pprint])
  (:import 
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.store FSDirectory)
           (org.apache.lucene.store RAMDirectory)
           (org.apache.lucene.util Version)
           (org.apache.lucene.index IndexWriterConfig)
           (org.apache.lucene.index IndexWriter)
           (org.apache.lucene.index DirectoryReader)
           (org.apache.lucene.search IndexSearcher)
           (org.apache.lucene.search Sort)
           (org.apache.lucene.search SortField)
           (org.apache.lucene.search SortField$Type)
           (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.document Document)
           (org.apache.lucene.document Field)
           (org.apache.lucene.document Field$Store)
           (org.apache.lucene.document Field$Index)
           (org.apache.lucene.document TextField)
           (org.apache.lucene.document StringField)
           (org.apache.lucene.document IntField)
           (org.apache.lucene.document FloatField)
           (java.util Collection Random)
           (java.io File File PushbackReader IOException FileNotFoundException )) 
  )

  

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

(defn strip-document 
  ([prefix doc]
   (cond 
     (string? doc) [prefix doc]
     (integer? doc) [prefix doc]
     (or (list? doc) (seq? doc) (vector? doc)) (flatten (map #(strip-document prefix %1) doc))
     (map? doc) (for [[k v] doc]
                  (flatten (strip-document (generate-key-name prefix k) v)) )))
  ([doc] (flatten (strip-document nil doc))))

(def counter (atom 0))
(swap! counter inc)


;; Array of strings should be indexed as array: [ "value" "value" "value" ]
;; Array of objects should be indexed as multiple arrays 
;; children$name [ "billy" "sally" ]
;; children$gender ["male" "female" ]
;; Note: This raises the usual issue of "how do I find people with children called billy who are male"
;; Answer: Write your own sodding index "billy_male" and search for that.
;;

(def results (strip-document {
                   :title "hello" 
                   :age 27 
                   :address 
                   { 
                    :line-one "3 Ridgeborough Hill" 
                    :post-code "IM4 7AS"}
                   :pets [ "bob" "harry" "dick"]
                   :children [
                              { :name "billy" :gender "male" }
                              { :name "sally" :gender "female" } ] }))

(defn two-at-a-time [remaining]
     (if (empty? remaining) nil
         (cons (take 2 remaining) 
         (lazy-seq (two-at-a-time (drop 2 remaining))))))

(defn put-pairs-into-obj [output item]
  (let [k (first item)
        v (last item)
        existing (get output k) ]
    (if existing 
      (if (coll? existing)
        (assoc output k (conj existing v))
        (assoc output k [existing v]))
      (assoc output k v))))

     
(.length "hello")

(defn map-to-lucene [input]
  (let [doc (Document.)
        fields 
       (filter boolean (for [[k v] input] 
         (cond
           (and (string? v) (< (.length v) 10)) (Field. k v StringField/TYPE_STORED) 
           (and (string? v) (>= (.length v) 10)) (Field. k v TextField/TYPE_STORED) 
           (integer? v) (Field. k v IntField/TYPE_STORED)
           :else nil
           )))]
    fields
;;    (doseq [f fields] (.add doc f))
;;    doc
    ))

;; Could I map fields twice? One for complex queries, one for exact matches?
;; I think I could!

#_ (pprint (map-to-lucene (reduce put-pairs-into-obj {} (two-at-a-time results))))
#_ (reduce put-pairs-into-obj {} (two-at-a-time results))

#_(strip-document {
                   :title "hello" 
                   :age 27 
                   :address 
                   { 
                    :line-one "3 Ridgeborough Hill" 
                    :post-code "IM4 7AS"}
                   :pets [ "bob" "harry" "dick"]
                   :children [
                              { :name "billy" :gender "male" }
                              { :name "sally" :gender "female" } ] 
                   }) 

