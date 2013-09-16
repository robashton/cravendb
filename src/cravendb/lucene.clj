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
   (put-entry [this ref-id content])
   (commit! [this]))

(defprotocol IndexReading
  (query [this options]))

(defrecord LuceneIndexReading [reader]
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
  (put-entry [this ref-id content]
    (let [doc (Document.)
          fields (for [[k v] content] 
                   (do
                     (println k v)
                     (Field. (pr-str k) (pr-str v) TextField/TYPE_STORED)))]
      (doseq [f fields] (.add doc f))
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
  (open-reader [this] (LuceneIndexReading. (DirectoryReader/open directory)))
  (close [this]
    (.close writer)
    (.close directory)))

(defn create-index [index-path]
  (let [analyzer (StandardAnalyzer. Version/LUCENE_CURRENT)
        directory (RAMDirectory.)
        config (IndexWriterConfig. Version/LUCENE_CURRENT analyzer)]
    (LuceneIndex. analyzer directory config)))

#_ (def index (create-index "foo"))
#_ (.close index)

#_ (with-open [writer (.open-writer index)]
      (-> writer
        (.put-entry "1" { "foo" "blah"})
        (.put-entry "2" { "foo" "cray"})
        (.put-entry "3" { "foo" "sneak"})
        (.put-entry "4" { "foo" "vlga"})
        (.put-entry "5" { "foo" "vlga"})
        (.put-entry "6" { "foo" "vlga"})
        (.put-entry "7" { "foo" "vlga"})
        (.put-entry "8" { "foo" "vlga"})
        (.commit!)
        (.close)))

#_ (with-open [reader (.open-reader index)]
      (doall (.query reader { :query "foo:[vlga]"})))

;; ;; Putting stuff into the index
;; #_ (def analyzer (StandardAnalyzer. Version/LUCENE_CURRENT))
;; #_ (def directory (RAMDirectory.))
;; #_ (.close directory)
;; #_ (def config (IndexWriterConfig. Version/LUCENE_CURRENT analyzer))
;; #_ (def writer (IndexWriter. directory config))
;; #_ (.close writer)
;; #_ 
;; #_ (def doc (Document.))
;; #_ (.add doc (Field. "foo" "searchtext", TextField/TYPE_STORED))
;; #_ (.add doc (Field. "__document_id" "1", TextField/TYPE_STORED))
;; #_ (.addDocument writer doc)
;; #_ 
;; #_ 
;; #_ ;; Querying the index
;; #_ (def reader (DirectoryReader/open directory))
;; #_ (.close reader)
;; #_ (def searcher (IndexSearcher. reader))
;; #_ 
;; #_ (def parser (QueryParser. Version/LUCENE_CURRENT "" analyzer))
;; #_ (def query (.parse parser "foo:searchtext"))
;; #_ 
;; #_ (def result (.search searcher query nil 1000))
;; #_ (count (.scoreDocs result))
;; 
;; 
;; 
;; #_ 
;; #_ (def indexed-documents (for [x (range 0 (count (.scoreDocs result)))] 
;;   (.doc searcher (.doc (aget (.scoreDocs result) x)))))
;; #_ 
;; #_ (def document-ids (map (fn [d] (.get d "__document_id")) indexed-documents))
;; #_ 
;; #_ (doall document-ids)
;; 
;; 
;; 
