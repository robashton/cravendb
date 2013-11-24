(ns cravendb.database)

(defprotocol DocumentDatabase
  (close 
    [this]
    "Closes this database and every resource associated with it")
  (load-document-metadata 
    [this id]
    "Retrieves all the metadata associated with a document")
  (query 
    [this opts]
    "Queries the database with the options specified in the map 'opts'
    The available options are
    :index \"default\" - The index to use for this query *advanced*
    :wait-duration 5   - How long to wait for the index to become non-stale (if wait is true)
    :wait false        - Whether to wait for the index to become non-stale
    :filter \"*\"      - The filter to apply to the query (by default, everything)
    :sort-order :asc   - The default sort-order (ascending)
    :sort-by nil       - The field to sort by (default, by best-match)
    :offset 0          - For paging, how many results to skip
    :amount 1000       - For paging, how many results to request")
  (clear-conflicts 
    [this id]
    "Clears all the conflicts associated with a document id")
  (conflicts 
    [this]
    "Retrieves a sequence of all the conflicts present in a database") 
  (put-document 
    [this id document metadata]
    "Puts a document into the database as a single atomic operation")
  (load-document 
    [this id]
    "Retrieves a document from the database by id") 
  (delete-document 
    [this id metadata]
    "Deletes a document from the database by id as a single atomic operation")
  (bulk [this operations])
  (put-index 
    [this index]
    "Puts an index into the database by id as a single atomic operation")
  (load-index-metadata 
    [this id]
    "Retrieves the metadata about an index by id")
  (delete-index 
    [this id]
    "Deletes an index and all associated data")
  (load-index 
    [this id]
    "Retrieves an index from the database by id"))

