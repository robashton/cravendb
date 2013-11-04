(ns cravendb.inflight-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])

  (:require
    [cravendb.documents :as docs]
    [cravendb.storage :as s]
    [cravendb.inflight :as inflight]
    [me.raynes.fs :as fs]
    ))

(describe 
  "In-flight transactions"
  (with db (s/create-in-memory-storage))
  (with te (inflight/create @db "root"))
  (after 
    (.close @db)
    (fs/delete-dir "testdir"))

  (describe "Adding a single document via the in-flight system"
    (with txid (inflight/open @te))
    (before
      (inflight/add-document @te @txid "doc-1" {:foo "bar"} {})
      (inflight/complete! @te @txid))
    (it "will write the document to storage"
      (should (docs/load-document @db "doc-1")))
    (it "will clear the document from the in-flight system"
      (should-not (inflight/is-registered? @te "doc-1")))
    (it "will clear the transaction from the in-flight system"
       (should-not (inflight/is-txid? @te @txid))))

  
  (describe "Deleting a single document via the in-flight system"
            
            )
  
 (describe "Adding a conflicting document via the in-flight system"
            
            ) 

  (describe "Deleting a conflicting document via the in-flight system"
            
            )

  (describe "Two clients adding a new document simultaneously"
            
            )

  (describe "Two clients modifying an existing simultaneously"
            
            )
  )





