[![Build Status](https://travis-ci.org/robashton/cravendb.png?branch=master)](https://travis-ci.org/robashton/cravendb)

# CravenDB

- A document database written in Clojure *for* Clojure
- It was originally based on RavenDB's design
- A rough to-do can be found in the file [todo.markdown](https://github.com/robashton/cravendb/blob/master/todo.markdown)
- So far we have
 - dynamic full-text queries against stored documents
 - custom indexes for advanced queries against stored documents
 - multi-document operations (transactional writes)
 - multi-master replication using vclocks for lineage checks
 - conflict-based concurrency control (when *strictly* necessary)
 - embedded database for local development
 - in-memory mode for fast testing
 - http server(s) for production

# Repos

### leiningen

    [cravendb "0.1.0-SNAPSHOT"]

### gradle

    compile "cravendb:cravendb:0.1.0-SNAPSHOT"

### maven

    <dependency>
      <groupId>cravendb</groupId>
      <artifactId>cravendb</artifactId>
      <version>0.1.0-SNAPSHOT</version>
    </dependency>

# Instructions

- Use the repl to explore
- Use the tests to verify (lein specs)
- Run the http server with 'lein run'

# License

Licensed under the EPL (see the file epl.html)
