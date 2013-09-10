(ns cravendb.lucene
  (:import (org.apache.lucene.index IndexReader$FieldOption IndexReader IndexWriter
                                    IndexWriter$MaxFieldLength Term CorruptIndexException)
           (org.apache.lucene.search IndexSearcher BooleanQuery
                                     PhraseQuery BooleanClause$Occur TermQuery)
           (org.apache.lucene.document Document Field Field$Store
                                       Field$Index DateTools
                                       DateTools$Resolution)
           (org.apache.lucene.analysis SimpleAnalyzer)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (org.apache.lucene.search.highlight QueryTermExtractor)
           (org.apache.lucene.store FSDirectory)
           (org.apache.lucene.queryParser QueryParser$Operator
                                          MultiFieldQueryParser)
           (org.apache.lucene.util Version)
           (java.util Collection Random)
           (java.io File File PushbackReader IOException FileNotFoundException )))




;;    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
;;
;;    // Store the index in memory:
;;    Directory directory = new RAMDirectory();
;;    // To store an index on disk, use this instead:
;;    //Directory directory = FSDirectory.open("/tmp/testindex");
;;    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer);
;;    IndexWriter iwriter = new IndexWriter(directory, config);
;;    Document doc = new Document();
;;    String text = "This is the text to be indexed.";
;;    doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
;;    iwriter.addDocument(doc);
;;    iwriter.close();
;;    
;;    // Now search the index:
;;    DirectoryReader ireader = DirectoryReader.open(directory);
;;    IndexSearcher isearcher = new IndexSearcher(ireader);
;;    // Parse a simple query that searches for "text":
;;    QueryParser parser = new QueryParser(Version.LUCENE_CURRENT, "fieldname", analyzer);
;;    Query query = parser.parse("text");
;;    ScoreDoc[] hits = isearcher.search(query, null, 1000).scoreDocs;
;;    assertEquals(1, hits.length);
;;    // Iterate through the results:
;;    for  int i = 0; i < hits.length; i++) {
;;      Document hitDoc = isearcher.doc(hits[i].doc);
;;      assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
;;     
;;    ireader.close();
;;    directory.close();
