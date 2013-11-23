# Details

### Consistency

For the operations "store/load/delete" and any combination of those using the bulk transaction calls, the effects are atomic immediately visible on a single node. They will always retrieve the latest versions of that information from a single node. We could say that this was ACID but that might annoy some people because in a multi-node set-up it is all eventually consistent.

On top of that, the secondary indexes used for querying are written as background operations meaning a query might not *always* return the latest results (for example under high write scenarios). They're pretty fast though.

### Concurrency

Each document written to a store has a vector clock associated with it, and this is used to track the history of that document. These are surfaced to the HTTP client as Etags and are available otherwise in the "metadata" associated with a document.

Submitting a document with an out of date vector clock, or an unknown lineage will cause a conflict. This can happen under the following circumstances

- Document A is written to Server A at exactly the same time as Document A is written to Server A by the client (this is stupidly *rare* on most systems)
- Document A is written to Server A at the same time as Document A is written to Server B by a client, then replication occurs
- Document A is written to Server A but with an old Etag/Vclock
- etc

A good description of vector clocks can be found on [Basho's site](http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/) and for all intents and purposes the use and behaviour of these clocks is identical except

- The client doesn't ever need to know about them unless it wants to be clear that it is updating a specific version of the document
- The server generates entity ids on a per-transaction basis (non-random)

When writing to a server without specifying the Etag, the server will write using the latest version of the vector clock it knows about and thus preserve lineage.

### Distribution

We're going with a CouchDB-ish model where writes are to a single server, and replication will occur eventually. There are plans to allow occasionally connected clients such as Clojurescript enabled browsers to be a part of this model and thus quorum writes wouldn't necessarily provide all that much on top of the experience.

Replication can be any topology of servers pointing to other servers and at present is completely manual. Auto discovery is in the pipeline.

### Philosophy

We don't care too much about throughput (although writing slow code should be avoided), what we care about is a good out of the box developer experience and we will be pushing to make the ops experience amazing too. Every question or confused user should be considered a bug.

### Release

Craven is still under development, and any documentation should be considered entirely transient.  That said - we have tried to make it obvious which APIs are for public consumption and playabouts *now* by adding docstrings and documentation on this site.
