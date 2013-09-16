(ns cravendb.lucene
  (:import 
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.store FSDirectory)
           (org.apache.lucene.store RAMDirectory)
           (org.apache.lucene.util Version)
           (org.apache.lucene.index IndexWriterConfig)
           (org.apache.lucene.index IndexWriter)
           (org.apache.lucene.index DirectoryReader)
           (org.apache.lucene.search IndexSearcher)
           (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.document Document)
           (org.apache.lucene.document Field)
           (org.apache.lucene.document TextField)
           (java.util Collection Random)
           (java.io File File PushbackReader IOException FileNotFoundException )))

;; Question: Is there a default one of these?
(defprotocol Closeable
  (close [this]))

(defprotocol IndexStore
  (open-writer [this])
  (open-reader [this]))

(defprotocol IndexWriting
   (put-entry! [this ref-id content])
   (commit! [this]))

(defprotocol IndexReading
  (query [this options]))

(defrecord LuceneIndexReading [reader analyzer]
  IndexReading
  Closeable
  (query [this options]
    (let [searcher (IndexSearcher. reader)
          parser (QueryParser. Version/LUCENE_CURRENT "" analyzer) 
          query (.parse parser (options :query))
          result (.search searcher query nil 1000)
          scoredocs (.scoreDocs result)
          docs (for [x (range 0 (count scoredocs))] 
                  (.doc searcher (.doc (aget scoredocs x))))]
          (map (fn [d] (.get d "__document_id")) docs)))
  (close [this]
    (.close reader)))

(defrecord LuceneIndexWriting [writer]
  IndexWriting
  Closeable
  (put-entry! [this ref-id content]
    (let [doc (Document.)
          fields (for [[k v] content] 
                   (if v
                     (Field. k v TextField/TYPE_STORED) 
                     nil))]
      (doseq [f (filter boolean fields)] (.add doc f))
      (.add doc (Field. "__document_id" ref-id TextField/TYPE_STORED))
      (.addDocument writer doc))
      this)
   (commit! [this] 
     (.commit writer)
     this)
   (close [this]
     (.close writer)))

(defrecord LuceneIndex [analyzer directory config]
  IndexStore
  Closeable
  (open-writer [this] (LuceneIndexWriting. (IndexWriter. directory config)))
  (open-reader [this] (LuceneIndexReading. (DirectoryReader/open directory) analyzer))
  (close [this] (.close directory)))

(defn create-index [file]
  (let [analyzer (StandardAnalyzer. Version/LUCENE_CURRENT)
        directory (FSDirectory/open file)
        config (IndexWriterConfig. Version/LUCENE_CURRENT analyzer)]
    (LuceneIndex. analyzer directory config)))

(defn create-memory-index []
  (let [analyzer (StandardAnalyzer. Version/LUCENE_CURRENT)
        directory (RAMDirectory.)
        config (IndexWriterConfig. Version/LUCENE_CURRENT analyzer)]
    (LuceneIndex. analyzer directory config)))
