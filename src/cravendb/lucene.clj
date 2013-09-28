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


(defprotocol IndexStore
  (open-writer [this])
  (open-reader [this]))

(defprotocol IndexReading
  (query [this query-string amount sort-field sort-order]))

(defprotocol IndexWriting
  (commit! [this]) 
  (delete-all-entries-for [this ref-id])
  (put-entry [this ref-id content]))

(defprotocol Closeable ;; How do I even??
  (close [this]))

(defrecord LuceneIndexWriting [writer analyzer]
  IndexWriting
  Closeable
  (delete-all-entries-for [this ref-id]
    (.deleteDocuments 
      writer 
       (.parse 
        (QueryParser. Version/LUCENE_CURRENT "" analyzer)
        (str "__document_id:" (QueryParser/escape ref-id))))
    this)
  (put-entry [this ref-id content]
    (let [doc (Document.)
          fields (for [[k v] content] 
                   (if v
                     (Field. k v TextField/TYPE_STORED) 
                     nil))]
      (doseq [f (filter boolean fields)] (.add doc f))
      (.add doc (Field. "__document_id" ref-id Field$Store/YES Field$Index/NOT_ANALYZED))
      (.addDocument writer doc))
      this) 
  (commit! [this]
    (.commit writer) 
    this)
  (close [this] 
    (.close writer)))



(defrecord LuceneIndexReading [reader analyzer]
  IndexReading
  Closeable
  (query [this query-string amount sort-field sort-order]
    (let [searcher (IndexSearcher. reader)
      parser (QueryParser. Version/LUCENE_CURRENT "" analyzer) 
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

  (close [this]
    (.close reader)))

(defrecord LuceneIndex [analyzer directory config]
  IndexStore
  Closeable
  (open-writer [this] (LuceneIndexWriting. 
                        (IndexWriter. directory config)
                         analyzer))
  (open-reader [this] (LuceneIndexReading. 
                        (DirectoryReader/open directory) analyzer))
  (close [this] 
    (.close directory)))

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
