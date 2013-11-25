# Bulk operations

While there are a pile of methods in the *cravendb.database* namespace we *could* use to play with this database, they are not really going to be used by most clients.

What we have is the notion of a "transaction", which is supposed to be used in per "unit of work" in the application (so per request or user action or whatever.)

We can open a transaction by passing in the [instance we created](./gettingstarted.html) - either *embedded* or *remote* it doesn't matter...

```
(require '[cravendb.transaction :as t])

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


### More than one change

Obviously we can now do more than one change at a time, and this operation is atomic too.

```clojure
; This will return the document because it is part of the current transaction
(-> (t/open instance)
    (t/store "doc-1" { :message "hello world" })
    (t/store "doc-2" { :message "hello alice" })
    (t/store "doc-3" { :message "hello bob" })
    (t/delete "doc-4")
    (t/commit!)
```

### Playing with this

The best bet is to load up the REPL and get a feel for how the transaction and database work together. Using the in-memory persistence is best for this.

Once comfortable with these basic crud operations, we might want to go and look at [querying](querying.html)
