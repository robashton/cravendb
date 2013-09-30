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
           (java.io File File PushbackReader IOException FileNotFoundException )))

(defn generate-key-name [prefix k]
  (clojure.string/replace (str (if prefix (str prefix "$")) k) ":" ""))

(defn strip-document 
  ([prefix doc]
   (cond 
     (string? doc) [prefix doc]
     (integer? doc) [prefix doc]
     (float? doc) [prefix doc]
     (decimal? doc) [prefix doc]
     (or (list? doc) (seq? doc) (vector? doc)) (flatten (map #(strip-document prefix %1) doc))
     (map? doc) (for [[k v] doc]
                  (flatten (strip-document (generate-key-name prefix k) v)) )))
  ([doc] (flatten (strip-document nil doc))))

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

(defn map-to-lucene 
  ([k v]
   (cond
    (and (string? v) (< (.length v) 10)) (StringField. k v Field$Store/NO) 
    (and (string? v) (>= (.length v) 10)) (TextField. k v Field$Store/NO) 
    (integer? v) (IntField. (str k) (int v) Field$Store/NO)
    (float? v) (FloatField. (str k) (float v) Field$Store/NO)
    (decimal? v) (FloatField. (str k) (bigdec v) Field$Store/NO)
    (coll? v) (map #(map-to-lucene k %1) v)
    :else v))
  ([input] 
   (flatten (filter 
    boolean 
    (for [[k v] input] (map-to-lucene k v))))))


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
                   :height 5.60
                   :address { 
                    :line-one "3 Ridgeborough Hill" 
                    :post-code "IM4 7AS" }
                   :pets [ "bob" "harry" "dick"]
                   :children [
                              { :name "billy" :gender "male" }
                              { :name "sally" :gender "female" } ] }))

#_ (pprint (map-to-lucene (reduce put-pairs-into-obj {} (two-at-a-time results))))
#_ (reduce put-pairs-into-obj {} (two-at-a-time results))

#_(strip-document {
                   :title "hello" 
                   :age 27 
                   :height 5.6
                   :address 
                   { 
                    :line-one "3 Ridgeborough Hill" 
                    :post-code "IM4 7AS"}
                   :pets [ "bob" "harry" "dick"]
                   :children [
                              { :name "billy" :gender "male" }
                              { :name "sally" :gender "female" } ] 
                   }) 


#_ (do
     (with-test-server 
      (fn []
        (client/put-index 
          "http://localhost:9000" 
          "by_username" 
          "(fn [doc] {\"username\" (:username doc)})")
        (client/put-document 
          "http://localhost:9000" 
          "1" { :username "bob"})
        (client/query 
          "http://localhost:9000" 
          { :query "username:bob" :index "by_username" :wait true})    
        (client/delete-document "http://localhost:9000" "1" )
        (println (count 
          (client/query 
            "http://localhost:9000" 
            { :query "username:bob" :index "by_username" :wait true})))))

      (with-test-server (fn [] 
        (client/put-document "http://localhost:9000" "1" "hello world")
        (client/delete-document "http://localhost:9000" "1")
        (println (client/get-document "http://localhost:9000" "1")))) 
     )  
