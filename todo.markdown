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

- Allow restricting indexing to documents with a prefix (cats-/dogs-)
- Default indexes created per prefix
- Options for Lucene
  - Capture field types so we know how to sort (or use Lucene options as above)
- Decide on how to expose Lucene queries to the consumer
- Think this should be largely automagic by default
- Meta data storage for documents

### Can wait

- Indexes can be properly broken, they need disabling and reporting
- Handle indexes that can't be compiled for some reason
- Modification of an index needs to mean re-indexing
  - Can I get away with renaming the folder to 'to-delete-blah'
  - Then deleting it?
  - This won't work for scheduled data, need indirection there at least
- Process to remove deleted documents from index
- Some form of concurrency check over transactions (MVCC most likely)
- The index engine shouldn't be swallowing agent exceptions
- Client should be handling HTTP results properly
- HTTP server should be sending back appropriate HTTP responses
- Documents should be validated as valid clojure objects by HTTP API
- HTTP API should be validating input
- Document storage should be responsible for serializing to string
- Allow indexes to be provided as actual functions (sfn macro) - this will make testing easier
- Look at threading indexing
