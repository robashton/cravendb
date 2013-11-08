(ns cravendb.brokenindexes-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core]
        )
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.database :as db]
            [cravendb.indexengine :as indexengine]
            [cravendb.indexstore :as indexes]
            [cravendb.query :as query]
            [cravendb.lucene :as lucene]
            [cravendb.storage :as s]))

(def write-three-documents 
  (fn [instance]
    (db/put-document instance "doc-1" { :title "hello" :author "rob"})
    (db/put-document instance "doc-2" { :title "morning" :author "vicky"})
    (db/put-document instance "doc-3" { :title "goodbye" :author "james"})))

(defn create-invalid-index []  
  {
   :id "invalid" 
   :map "(fn [doc] {\"hello\" ((:blah doc) :foo)})"
   })

(defn create-valid-index []  
  {
   :id "valid" 
   :map "(fn [doc] {\"hello\" (:author doc)})"
})

(defn create-test-indexes [] [ (create-invalid-index) (create-valid-index) ])

(describe "Marking an index as failed"
  (it "will mark an index as failed if it throws exceptions"
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (write-three-documents instance)
      (db/put-index instance (create-invalid-index))
      (indexing/wait-for-index-catch-up storage "invalid" 5)
      (should (indexes/is-failed storage "invalid")))))

  (it "will not use this index in further indexing processes"
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (write-three-documents instance)
      (db/put-index instance (create-invalid-index))
      (indexing/wait-for-index-catch-up storage)
      (let [last-synctag (s/last-synctag-in storage)]
        (write-three-documents instance)
        (indexing/wait-for-index-catch-up storage "default" 5)
        (should= last-synctag (indexes/get-last-indexed-synctag-for-index storage "invalid")))))) 

  (it "will carry on indexing non-broken indexes"
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (write-three-documents instance)
      (db/put-index instance (create-invalid-index))
      (db/put-index instance (create-valid-index))
      (indexing/wait-for-index-catch-up storage)
      (write-three-documents instance)
      (indexing/wait-for-index-catch-up storage "valid" 5)
      (should= (s/last-synctag-in storage) (indexing/last-indexed-synctag storage))))))

(describe "Resetting an index"
  (it "will reset the last indexed synctag for that index"
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (write-three-documents instance)
      (db/put-index instance (create-invalid-index))
      (indexing/wait-for-index-catch-up storage)
      (with-open [tx (s/ensure-transaction storage)]
        (s/commit! (indexes/reset-index tx "invalid")))
      (should= (zero-synctag) (indexes/get-last-indexed-synctag-for-index storage "invalid")))) )
  (it "will mark the index as not failed"
    (with-full-setup (fn [{:keys [storage] :as instance}]
      (write-three-documents instance)
      (db/put-index instance (create-invalid-index))
      (indexing/wait-for-index-catch-up storage)
      (with-open [tx (s/ensure-transaction storage)]
        (s/commit! (indexes/reset-index tx "invalid")))
      (should-not (indexes/is-failed storage "invalid"))))))
