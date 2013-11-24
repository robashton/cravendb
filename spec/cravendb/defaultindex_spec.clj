(ns cravendb.defaultindex-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.querylanguage])

  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexengine :as indexengine]
            [cravendb.query :as query]
            [cravendb.database :as database]
            [cravendb.storage :as s]))

(defn add-standard-data-set [instance]
  (database/put-document instance "docs-1" { "name" "zebra" "description" "the zebra is a horse like creature with black and white stripes" "number" 25})
  (database/put-document instance "docs-2" { "name" "aardvark" "description" "the aardvaark has a lot of a's in his name, kinda silly really" "number" 500})
  (database/put-document instance "docs-3" { "name" "giraffe" "description" "the giraffe looks a bit silly with its big long neck" "number" 100})
  (database/put-document instance "docs-4" { "name" "anteater" "description" "the anteater has a stupid looking long nose" "number" 50})
  (database/put-document instance "docs-5" { :name "Air" :album "Talkie Walkie"}))


(defn extract-name-from-result [results]
  (get (first results) "name")) 

(describe "default index"
  (it "will search on exact short strings"
     (with-full-setup
      (fn [{:keys [storage index-engine] :as instance}]
        (add-standard-data-set instance)
        (indexing/wait-for-index-catch-up storage 50)
        (should= "zebra" 
          (extract-name-from-result 
            (database/query instance  :index "default" :filter (=? "name" "zebra")))))))
   (it "will search on partial short strings"
     (with-full-setup
      (fn [{:keys [storage index-engine] :as instance}]
        (add-standard-data-set instance)
        (indexing/wait-for-index-catch-up storage 50)
        (should= "zebra" 
          (extract-name-from-result 
            (database/query instance :index "default" :filter (starts-with? "name" "zeb")))))))
    (it "will do word-based matching on long strings"
     (with-full-setup
      (fn [{:keys [storage index-engine] :as instance}]
        (add-standard-data-set instance)
        (indexing/wait-for-index-catch-up storage 50)
        (should= "zebra" 
          (extract-name-from-result 
            (database/query instance :index "default" :filter (has-word? "description" "horse")) )))))
          
    (it "will do partial-word-based matching on long strings"
     (with-full-setup
      (fn [{:keys [storage index-engine] :as instance}]
        (add-standard-data-set instance)
        (indexing/wait-for-index-catch-up storage 50)
        (should= "anteater" 
          (extract-name-from-result 
            (database/query instance :index "default" :filter (has-word-starting-with? "description" "stup")))))))

     (it "will do exact matching on integers"
     (with-full-setup
      (fn [{:keys [storage index-engine] :as instance}]
        (add-standard-data-set instance)
        (indexing/wait-for-index-catch-up storage 50)
        (should= "anteater" 
          (extract-name-from-result 
            (database/query instance  :index "default" :filter (=? "number" 50) ) )))))

     (it "will do less than matching on integers"
     (with-full-setup
      (fn [{:keys [storage index-engine] :as instance}]
        (add-standard-data-set instance)
        (indexing/wait-for-index-catch-up storage 50)
        (should= "zebra" 
          (extract-name-from-result 
            (database/query instance :index "default" :filter (<? "number" 30)) )))))

      (it "will do greater than matching on integers"
        (with-full-setup
      (fn [{:keys [storage index-engine] :as instance}]
          (add-standard-data-set instance)
          (indexing/wait-for-index-catch-up storage 50)
          (should= "aardvark" 
            (extract-name-from-result 
              (database/query instance  :index "default" :filter (>? "number" 499) ) )))))

     (it "will exclude the literal from the less than range"
     (with-full-setup
      (fn [{:keys [storage index-engine] :as instance}]
        (add-standard-data-set instance)
        (indexing/wait-for-index-catch-up storage 50)
        (should= 1 
          (count (database/query instance  :index "default" :filter (<? "number" 50) ))))))

      (it "will exclude the literal from the greater than range "
        (with-full-setup
          (fn [{:keys [storage index-engine] :as instance}]
            (add-standard-data-set instance)
            (indexing/wait-for-index-catch-up storage 50)
            (should= 1
              (count (database/query instance  :index "default" :filter (>? "number" 100) ) )))))

      (it "will do less than or equal than matching on integers"
        (with-full-setup
          (fn [{:keys [storage index-engine] :as instance}]
            (add-standard-data-set instance)
            (indexing/wait-for-index-catch-up storage 50)
            (should= "zebra" 
              (extract-name-from-result 
                (database/query instance  :index "default" :filter (<=? "number" 25) ) )))))

        (it "will do greater than or equal than matching on integers"
         (with-full-setup
          (fn [{:keys [storage index-engine] :as instance}]
            (add-standard-data-set instance)
            (indexing/wait-for-index-catch-up storage 50)
            (should= "aardvark" 
              (extract-name-from-result 
                (database/query instance  :index "default" :filter (>=? "number" 500) ) )))))

      (it "will allow queries against symbol based keys"
        (with-full-setup
          (fn [{:keys [storage index-engine] :as instance}]
            (add-standard-data-set instance)
            (indexing/wait-for-index-catch-up storage 50)
            (should== { :name "Air" :album "Talkie Walkie" } 
                      (first 
                        (database/query instance  :index "default" :filter (=? :name "Air") ) )))))

      (it "will allow queries inside collections"
        (with-full-setup
          (fn [{:keys [storage index-engine] :as instance}]
            (database/put-document instance "1" { "name" "bob" :collection [ "one" "two" "three"]})
            (database/put-document instance "2" { "name" "alice" :collection [ "two" "three" "four"]})
            (indexing/wait-for-index-catch-up storage 50)
            (should= "bob"
              (extract-name-from-result
                (database/query instance  :index "default" :filter (has-item? :collection "one")))))))

      (it "will allow multiple claused joined by an 'and'"
        (with-full-setup
          (fn [{:keys [storage index-engine] :as instance}]
            (database/put-document instance "1" { "name" "bob" :collection [ "one" "two" "three"]})
            (database/put-document instance "2" { "name" "alice" :collection [ "two" "three" "four"]})
            (database/put-document instance "3" { "name" "alice" :collection [ "one" "four"]})
            (indexing/wait-for-index-catch-up storage 50)
            (should== { "name" "alice" :collection [ "two" "three" "four"]}
              (first 
                (database/query instance  :index "default" :filter (AND (=? "name" "alice") (has-item? :collection "two")) ))))))  

      (it "will allow multiple claused joined by an 'or'"
        (with-full-setup
          (fn [{:keys [storage index-engine] :as instance}]
            (database/put-document instance "1" { "name" "bob" :collection [ "one" "two" "three"]})
            (database/put-document instance "2" { "name" "alice" :collection [ "two" "three" "four"]})
            (database/put-document instance "3" { "name" "alice" :collection [ "one" "four"]})
            (indexing/wait-for-index-catch-up storage 50)
            (should= 3
              (count (database/query instance  :index "default" :filter (OR (has-item? :collection "four") (has-item? :collection "two")) )))))) 

      (it "will not include results when using a 'not'"
        (with-full-setup
          (fn [{:keys [storage index-engine] :as instance}]
            (database/put-document instance "1" { "name" "bob" :collection [ "one" "two" "three"]})
            (database/put-document instance "2" { "name" "alice" :collection [ "two" "three" "four"]})
            (database/put-document instance "3" { "name" "alice" :collection [ "one" "four"]})
            (indexing/wait-for-index-catch-up storage 50)
            (should= "bob"
              (extract-name-from-result
                (database/query instance  :index "default" :filter (NOT (=? "name" "alice")) ))))))

)

