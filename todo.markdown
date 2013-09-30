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

- Stick a license in the repo!!
- Default indexes created per prefix
  - Default options
  - Default index to query
  - Can I have a consistent query language between these and custom maps?
  - Decide on how to expose Lucene queries to the consumer ^^
    - Looks like a custom query language is more appropriate given auto-indexing

- Options for Lucene
  - Capture field types so we know how to sort (or use Lucene options as above)

### Can wait

- Consider those small functions as inline functions
- Tidy up my namespaces - use the colon prefixed stuffs
  - Try to get rid of the uses
- Safe reading of maps

- Etag generation is not thread safe, it needs to be a shared incrementable atom
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

- Look at ex-info/ex-data
- Meta data storage for documents
- Try to use the Clojure string library rather than all the .startsWith etc
- Look at using reducers to queue up the indexing operation

###

To look at - Jig (repl restart helpers)
           - Route One - generating URLs
           - clj-http library 
           - Use clout
           - See if slingshot is deprecated or not
           - Ribol - Have a gander, but only a gander
           - Look at lamina for async operations           

