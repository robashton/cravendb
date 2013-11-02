(ns cravendb.synctags-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])

  (:require [cravendb.storage :as s]
            [cravendb.documents :as docs]
            [cravendb.database :as db]))

(describe "Sync tags"
  (it "will have an synctag starting at zero before anything is written"
    (inside-tx (fn [db]
      (should= (synctag-to-integer (s/last-synctag-in db)) 0))))

  (it "Will have an synctag greater than zero after committing a single document"
    (with-full-setup (fn [instance]
      (db/put-document instance "1" "hello")
      (should 
        (< 0 (synctag-to-integer (s/last-synctag-in (:storage instance))))))))

  (it "links an synctag with a document upon writing"
    (inside-tx (fn [tx]
      (should= "1337" (-> tx
          (docs/store-document "1" "hello" {:synctag "1337"})
          (docs/synctag-for-doc "1"))))))

  (it "can retrieve documents written since an synctag"
    (with-db (fn [db]
      (with-open [tx (s/ensure-transaction db)] 
        (->
          (docs/store-document tx "1" "hello" {:synctag (integer-to-synctag 1)}) 
          (docs/store-document "2" "hello" {:synctag (integer-to-synctag 2)})
          (docs/store-document "3" "hello" {:synctag (integer-to-synctag 3)})
          (s/commit!))
        (with-open [tx (s/ensure-transaction db)]
          (with-open [iter (s/get-iterator tx)]
            (should== '("2" "3") (docs/iterate-synctags-after iter (integer-to-synctag 1)))))))))) 
