(ns cravendb.inflight-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])

  (:require
    [cravendb.vclock :as v]
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
    (with txid (inflight/open @te))
    (before
      (with-open [tx (s/ensure-transaction @db)] 
        (s/commit! (docs/store-document tx "doc-1" {} {}))
      (inflight/delete-document @te @txid "doc-1" {})
      (inflight/complete! @te @txid)))
    (it "will write the document to storage"
      (should-not (docs/load-document @db "doc-1")))
    (it "will clear the document from the in-flight system"
      (should-not (inflight/is-registered? @te "doc-1")))
    (it "will clear the transaction from the in-flight system"
       (should-not (inflight/is-txid? @te @txid))))
  
  (describe "Two clients writing at the same time without specifying history"
    (with txid-1 (inflight/open @te))
    (with txid-2 (inflight/open @te))
    (before
      (inflight/add-document @te @txid-1 "doc-1" { :name "1"} {})
      (inflight/add-document @te @txid-2 "doc-1" { :name "2"} {})
      (inflight/complete! @te @txid-2)
      (inflight/complete! @te @txid-1))
      (it "will write the first document"
        (should== {:name "1"} (docs/load-document @db "doc-1")))
      (it "will generate a conflict document for the second write"
        (should== {:name "2"} (first (map :data (docs/conflicts @db))))))

  (describe "Two clients writing at the same time, no history specified, second client deletes"
    (with txid-1 (inflight/open @te))
    (with txid-2 (inflight/open @te))
    (before
      (with-open [tx (s/ensure-transaction @db)] 
        (s/commit! (docs/store-document tx "doc-1" {:name "original"} {})))
      (inflight/add-document @te @txid-1 "doc-1" { :name "1"} {})
      (inflight/delete-document @te @txid-2 "doc-1" {})
      (inflight/complete! @te @txid-2)
      (inflight/complete! @te @txid-1))
      (it "will write the first document"
        (should= { :name "1" } (docs/load-document @db "doc-1")))
      (it "will generate a conflict document for the :delete"
        (should= :deleted (first (map :data (docs/conflicts @db))))))

  (describe "A client writing with no history when a document already exists"
    (with txid (inflight/open @te))
    (with old-history (v/next "boo" (v/new)))
    (before
      (with-open [tx (s/ensure-transaction @db)] 
        (s/commit! (docs/store-document tx "doc-1" {:name "old-doc"} { :history @old-history}))
      (inflight/add-document @te @txid "doc-1" {:name "new-doc"} {})
      (inflight/complete! @te @txid)))
    (it "will write the document to storage"
      (should== { :name "new-doc" } (docs/load-document @db "doc-1")))
    (it "will clear the document from the in-flight system"
      (should-not (inflight/is-registered? @te "doc-1")))
    (it "will be a descendent of the old document"
      (should (v/descends? 
                (:history (docs/load-document-metadata @db "doc-1"))
                @old-history)))
    (it "will not generate a conflict"
        (should= 0 (count (docs/conflicts @db)))))

  (describe "Two clients both providing history trying to write a document"
    (with txid-1 (inflight/open @te))
    (with txid-2 (inflight/open @te))
    (with old-history (v/next "boo" (v/new)))
    (before
       (with-open [tx (s/ensure-transaction @db)] 
        (s/commit! (docs/store-document tx "doc-1" {:name "old-doc"} { :history @old-history})))
      (inflight/add-document @te @txid-1 "doc-1" { :name "1"} {})
      (inflight/add-document @te @txid-2 "doc-1" { :name "2"} {})
      (inflight/complete! @te @txid-2)
      (inflight/complete! @te @txid-1))
      (it "will write the first document"
        (should== {:name "1"} (docs/load-document @db "doc-1")))
      (it "will write the first document as a descendent of the orignal"
        (should (v/descends? 
                (:history (docs/load-document-metadata @db "doc-1"))
                @old-history)))
      (it "will generate a conflict for the second document"
          
          )
      (it "will write the conflict as a descendent of the original"

            )

  (describe "A client providing an out of date history when writing"   )))  





