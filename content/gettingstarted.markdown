# Getting started

There are a few ways we can use Craven, in order of increasing complexity

- As an in-process embedded database
- As a remote single node http server
- As a remote cluster of http nodes

This little getting started guide will focus on the first one, so we can get to grips with "how Craven works", the client API for the above scenarios are the same apart from the set-up.

### Getting it into your project

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

### "Connecting" to a database

No matter how we use Craven, we need to create a handle to the database. This is designed to be created once on application start-up and then used for the lifetime of the application.

To create an in-memory database, we can use the code

```clojure
(require '[cravendb.embedded :as embedded])
(def instance (embedded/create))
```

To create an embedded database that is backed to disk

```
(require '[cravendb.embedded :as embedded])
(def instance (embedded/create :path "/some/path"))
```

And to create a database that is talking to a remote server

```
(require '[cravendb.remote :as remote])
(def instance (remote/create :href "http://localhost:8000"))
```

Beyond this, everything described is the same regardless. The in-memory storage is primarily so that tests can be ran quickly and so that we can try things out in the REPL without worrying about making a mess.

This instance is "closeable", and should be closed at the end of the application/test/whatever.

```
(.close instance)
```

Obviously this means we can run our application in a with-open block quite happily

```
(with-open [instance (embedded/create :path "somepath")]
  (our-app-code instance))
```

## Basic operations

Basic operations supported by our database can be found in the namespace *(cravendb.database)* although most of the time we won't be using this; each operation in this namespace is atomic and syncrhonous in nature.

- The id is always a string (below, this is *doc-1*)
- Documents can be any valid Clojure object (strings, lists, sequences, maps, etc) - they should all round-trip happily

### Storing a document

```clojure
(require '[cravendb.database :as db])

; PUT /document/doc-1 
(db/put-document instance "doc-1" { :hello "bob" :address { :street "jekyll street" } })
```

### Loading a document

```clojure
(require '[cravendb.database :as db])

; GET /document/doc-1 
(db/load-document instance "doc-1")
```

### Deleting a document

```clojure
(require '[cravendb.database :as db])

; DELETE /document/doc-1
(db/delete-document instance "doc-1")
```

Next, we might want to look at [Bulk operations](./bulkoperations.html)

