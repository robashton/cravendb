# Implemented thus far

- Transactional key-value storage on top of LevelDB
- Document storage on top of that
- Index storage on top of documents
- Etags per documents kept per version
- Wrapper around Lucene for index content
- Indexing process that executes 'maps' documents into Lucene
- Querying against the indexes, loading documents that match
- HTTP API around the above
- Basic client API around the HTTP API
- Bulk imports
- A rudimentary client-side session helper
- Paging through query results
- Mapping happens in chunks so safe-shutdown can be had during repl tests

# Pending/debt/etc

### Immediate priority

- NOTHING AT ALL YAY

### Can wait

- Allow restricting indexing to documents with a prefix (cats-/dogs-)
- Modification of an index needs to mean re-indexing
  - Can I get away with renaming the folder to 'to-delete-blah'
  - Then deleting it?
  - This won't work for scheduled data, need indirection there at least
- Options for Lucene
  - Capture field types so we know how to sort (or use Lucene options as above)
- Decide on how to expose Lucene queries to the consumer
- Process to remove deleted documents from index
- Some form of concurrency check over transactions (MVCC most likely)
- Handle errors during indexing so it doesn't infini-loop
- The index engine shouldn't be swallowing agent exceptions
- Client should be handling HTTP results properly
- HTTP server should be sending back appropriate HTTP responses
- Documents should be validated as valid clojure objects by HTTP API
- HTTP API should be validating input
- Document storage should be responsible for serializing to string
- Allow indexes to be provided as actual functions (sfn macro) - this will make testing easier

