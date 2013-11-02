[![Build Status](https://travis-ci.org/robashton/CravenDB.png?branch=master)](https://travis-ci.org/robashton/CravenDB)

# CravenDB

- A document database written in Clojure for learning purposes
- It is based loosely on RavenDB's design
- A rough to-do can be found in the file "todo.markdown"
- So far we have
 - dynamic full-text queries against stored documents
 - custom indexes for advanced queries against stored documents
 - multi-document operations (transactional writes)
 - multi-master replication using vclocks for lineage checks
 - conflict-based concurrency control (when *strictly* necessary)
 - in-memory mode for fast testing
 - http server for remote cluster use
 - embedded database for local development

# Instructions

- Use the repl to explore
- Use the tests to verify (lein specs)
- Run the http server with 'lein run'

# License

Licensed under the EPL (see the file epl.html)
