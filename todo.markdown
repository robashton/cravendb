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
- synctag generation is incremental and thread-safe for a single node
- vector clocks used to track document history and do cross-node conflict avoidance
- MVCC with manual conflict resolution
- Travis CI set-up

# TODO

### Immediate issues

- Handle indexes that can't be compiled for some reason
- Indexes should ideally be defended from concurrency too
- Failed tasks

### Pending tasks

- Client should be handling HTTP results properly
- Need some tests for three-way replication checking etc
- Automatic peer discovery
- Use long-polling for replication/streaming
- Formalise in-memory/leveldb split

### Pending debt/questions/bugs

- How best to deal with conflicts in a multi-master scenario? At the moment we remain inconsistent
- The indexing engine isn't as robust as I'd like it to be
- Can I use core.async to help make the index engine code better?
- Can I use leveldb's write/read options to do my work for me?
- I need to show sensible parsing errors on parsing failure
- Consider all those small functions as inline functions
- Allow indexes to be provided as actual functions (sfn macro) - this will make testing easier
- Look at threading indexing
- Try to use the Clojure string library rather than all the .startsWith etc
- Look at using reducers to queue up the indexing operation (currently creating new sequences per transformation)
- Something is wrong with my usage of leveldb, tests crash with pthread error occasionally

### To look at 

- Jig (repl restart helpers)
- Route One - generating URLs
- clj-http library 
- Use clout
- See if slingshot is deprecated or not
- Ribol - Have a gander, but only a gander
- Look at lamina for async operations           
- Might want to use Quartz for scheduling things in the future (there's a joke there somewhere)

