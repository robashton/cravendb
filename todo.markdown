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
- Dynamic queries (no creation of index required!)
- A temporary query language against documents
- Etag generation is incremental and thread-safe for a single node
- MVCC with manual conflict resolution
- Travis CI set-up

# TODO

### Immediate priority

- Deleting indexes should ideally delete the persistence for those indexes
- Handle indexes that can't be compiled for some reason

### Pending tasks

- I should have an in-flight transaction system for dealing with concurrency
- Process to remove deleted documents from index
- Client should be handling HTTP results properly
- HTTP API should be validating input
- Cross-node-safe etag generation (vector clocks?)
- Meta data storage for documents
- Bulk operations don't take into account etags on put

### Pending debt/questions/bugs

- Can I use leveldb's write/read options to do my work for me?
- Need to formalise that database thing and whether it supports transactions
- I need to show sensible parsing errors on parsing failure
- Consider all those small functions as inline functions
- Safe reading of maps
- Allow indexes to be provided as actual functions (sfn macro) - this will make testing easier
- Look at threading indexing
- Try to use the Clojure string library rather than all the .startsWith etc
- The indexing process *could* miss out newly written documents in rare cases, should make this not possible
- Look at using reducers to queue up the indexing operation (currently creating new sequences per transformation)

### To look at 

- Jig (repl restart helpers)
- Route One - generating URLs
- clj-http library 
- Use clout
- See if slingshot is deprecated or not
- Ribol - Have a gander, but only a gander
- Look at lamina for async operations           

