(ns cravendb.documents-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.indexes]))


(describe "Map indexes"
  (it "can put and get an index"
    (with-db (fn [db]
      (store-map-index db "1" "(fn [item] item)") 
      (should (= (load-map-index db "1") "(fn [item] item)"))))))
  (it "assigns an id for an index when put"
      (with-db 
        (fn [db]
          (store-map-index db "hello" "(fn [item] item)")
          (should (id-for-index db "hello"))))))



