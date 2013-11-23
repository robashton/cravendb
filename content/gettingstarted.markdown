# Getting started

There are a few ways we can use Craven, in order of increasing complexity

- As an in-process embedded database
- As a remote single node http server
- As a remote cluster of http nodes

This little getting started guide will focus on the first one, so we can get to grips with "how Craven works", the client API for the above scenarios are the same apart from the set-up.

### Getting it into your project

I'll have it in a maven thingy, honest

### "Connecting" to a database

No matter how we use Craven, we need to create a handle to the database. This is designed to be created once on application start-up and then used for the lifetime of the application.

```clojure
(require '[cravendb.database :as db])
```

To create an in-memory database, we can use the code

```clojure
(def instance (db/create))
```

To create a database that is backed to disk

```
(def instance (db/create :path "somedb"))
```

And to create a database that is talking to a remote server

```
(def instance (db/create :href "http://localhost:8000"))
```

Beyond this, everything described is the same regardless. The in-memory storage is primarily so that tests can be ran quickly and so that we can try things out in the REPL without worrying about making a mess.

This instance is "closeable", and should be closed at the end of the application/test/whatever.

```
(.close instance)
```

Obviously this means we can run our application in a with-open block quite happily

```
(with-open [instance (db/create :path "somepath")]
  (our-app-code instance))
```

### Round-tripping our first document

While there are a pile of methods in the db namespace we *could* use to play with this database, they are not really meant to be used in the application.

What we have is the notion of a "transaction", which is supposed to be used in per "unit of work" in the application (so potentially per request or whatever)

```clojure
  (require '[cravendb.transaction :as t])
```

We can open a transaction by passing in the instance we created earlier

```
(def tx (t/open instance))
```

To save a document to the datastore, we'd chain a collection of calls to this transaction. Documents can be any valid clojure structure, with nested maps and lists and sequences and symbols and things will carry on working tickety boo.

```clojure
; Opens a transaction, stores a document and commits the transaction
  (-> (t/open instance)
      (t/store "doc-1" { :message "hello world" })
      (t/commit!))
```


As can be seen, the transaction itself is immutable, and all the methods that perform an operation simply return a new version of the transaction with the operation attached to it.

Loading a document from the transaction is quite simple too
 
```clojure
; Assuming we have a transaction called tx, this will load our saved document
  (t/load tx "doc-1")
``` 

The transaction will attempt to show the current state of the world *as if the transaction was already committed*, this means that uncommitted documents that are a part of this transaction will be returned from 'load' calls.

```clojure
; This will return the document because it is part of the current transaction
(-> (t/open instance)
    (t/store "doc-1" { :message "hello world" })
    (t/load "doc-1"))
```

### Deleting a document

```clojure
(-> (t/open instance)
    (t/delete "doc-1")
    (t/commit!))
```

In the same way above, a transaction will return 'nil' for documents that are marked as deleted as part of this transaction.

```clojure
; This will return nil because the document is deleted in this transaction
(-> (t/open instance)
    (t/delete "doc-1")
    (t/load "doc-1"))
```

### Playing with this

The best bet is to load up the REPL and get a feel for how the transaction and database work together. Using the in-memory persistence is best for this.

Once comfortable with these basic crud operations, we might want to go and look at [querying](/querying.html)
