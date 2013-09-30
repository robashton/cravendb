(ns cravendb.lucene
  (use [clojure.tools.logging :only (info error debug)])
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
           (java.util Collection Random)
           (java.io File File PushbackReader IOException FileNotFoundException )))

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

(defn delete-all-entries-for [index ref-id]
  (.deleteDocuments 
    (:writer index) 
      (.parse 
        (QueryParser. Version/LUCENE_CURRENT "" (:analyzer index))
        (str "__document_id:" (QueryParser/escape ref-id))))
  index)

(defn put-entry [index ref-id content]
  (let [doc (Document.)
      fields (for [[k v] content] 
                (if v
                  (Field. k v TextField/TYPE_STORED) 
                  nil))]
    (doseq [f (filter boolean fields)] (.add doc f))
    (.add doc (Field. "__document_id" ref-id Field$Store/YES Field$Index/NOT_ANALYZED))
    (.addDocument (:writer index) doc))
  index) 

(defn commit! [index]
  (.commit (:writer index)) 
  index)


(defn query [index query-string amount sort-field sort-order]
    (let [searcher (IndexSearcher. (:reader index))
      parser (QueryParser. Version/LUCENE_CURRENT "" (:analyzer index)) 
      query (.parse parser query-string)
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
