(ns cravendb.lucene
  (:use [clojure.tools.logging :only (info error debug)])
  (:require [cravendb.queryparsing :as qp])
  (:import 
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.store FSDirectory RAMDirectory)
           (org.apache.lucene.util Version)
           (org.apache.lucene.index IndexWriterConfig IndexWriter DirectoryReader)
           (org.apache.lucene.search IndexSearcher Sort SortField SortField$Type)
           (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.document Document Field Field$Store Field$Index 
                                      TextField IntField FloatField StringField)))

(defprotocol Closeable
  (close [this]))

(defrecord LuceneIndexWriting [writer analyzer]
  Closeable
  (close [this] 
    (.close writer)))
(defrecord LuceneIndexReading [reader analyzer]
  Closeable
  (close [this]
    (.close reader)))
(defrecord LuceneIndex [analyzer directory config]
  Closeable
  (close [this] 
    (.close directory)))

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
   (let [result (flatten (filter 
    boolean 
    (for [[k v] input] (map-to-lucene k v))))]
     (debug "Created lucene fields for" input "to" result)
     result)))

(defn delete-all-entries-for [index ref-id]
  (.deleteDocuments 
    (:writer index) 
      (.parse 
        (QueryParser. Version/LUCENE_CURRENT "" (:analyzer index))
        (str "__document_id:" (QueryParser/escape ref-id))))
  index)

(defn document-id-field [ref-id]
  (Field. "__document_id" ref-id Field$Store/YES Field$Index/NOT_ANALYZED))

(defn put-entry [index ref-id content]
  (let [doc (Document.)]
    (doseq [f (map-to-lucene content)] (.add doc f))
    (.add doc (document-id-field ref-id))
    (.addDocument (:writer index) doc))
  index) 

(defn commit! [index]
  (.commit (:writer index)) 
  index)

(defn query [index query-string amount sort-field sort-order]
    (let [searcher (IndexSearcher. (:reader index))
      query (qp/to-lucene query-string) 
      sort-options (if sort-field 
                       (Sort. (SortField. 
                                sort-field 
                                (SortField$Type/STRING)
                                (if (= sort-order :asc) false true))) 
                        (Sort.))
      result (.search searcher query amount sort-options)
      scoredocs (.scoreDocs result)
      docs (for [x (range 0 (count scoredocs))] 
              (.doc searcher (.doc (aget scoredocs x))))]
              (distinct (map (fn [d] (.get d "__document_id")) docs)))) 


(defn open-writer [index] (LuceneIndexWriting. 
                      (IndexWriter. (:directory index) (:config index))
                        (:analyzer index)))

(defn open-reader [index] (LuceneIndexReading. 
                      (DirectoryReader/open (:directory index)) (:analyzer index)))

(defn create-index [file]
  (let [analyzer (StandardAnalyzer. Version/LUCENE_CURRENT)
        directory (FSDirectory/open file)
        config (IndexWriterConfig. Version/LUCENE_CURRENT analyzer) ]
    (LuceneIndex. analyzer directory config)))

(defn create-memory-index []
  (let [analyzer (StandardAnalyzer. Version/LUCENE_CURRENT)
        directory (RAMDirectory.)
        config (IndexWriterConfig. Version/LUCENE_CURRENT analyzer)]
    (LuceneIndex. analyzer directory config)))
