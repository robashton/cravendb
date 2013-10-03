(ns cravendb.defaultindex-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.querylanguage])

  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexengine :as indexengine]
            [cravendb.query :as query]
            [cravendb.storage :as s]))

(defn add-standard-data-set [db]
  (with-open [tx (s/ensure-transaction db)] 
    (-> tx
      (docs/store-document "docs-1" (pr-str { "name" "zebra" "description" "the zebra is a horse like creature with black and white stripes" "number" 25}))
      (docs/store-document "docs-2" (pr-str { "name" "aardvark" "description" "the aardvaark has a lot of a's in his name, kinda silly really" "number" 500}))
      (docs/store-document "docs-3" (pr-str { "name" "giraffe" "description" "the giraffe looks a bit silly with its big long neck" "number" 100}))
      (docs/store-document "docs-4" (pr-str { "name" "anteater" "description" "the anteater has a stupid looking long nose" "number" 50}))
      (docs/store-document "docs-5" (pr-str { :name "Air" :album "Talkie Walkie"}))
      (s/commit!))))


(defn extract-name-from-result [results]
  (get (first (map read-string results)) "name")) 

(describe "default index"
  (it "will search on exact short strings"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= "zebra" 
          (extract-name-from-result 
            (query/execute db engine { :index "default" :query (=? "name" "zebra")}))))))
   (it "will search on partial short strings"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= "zebra" 
          (extract-name-from-result 
            (query/execute db engine { :index "default" :query (starts-with? "name" "zeb")}))))))
    (it "will do word-based matching on long strings"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= "zebra" 
          (extract-name-from-result 
            (query/execute db engine { :index "default" :query (has-word? "description" "horse")}) )))))
          
    (it "will do partial-word-based matching on long strings"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= "anteater" 
          (extract-name-from-result 
            (query/execute db engine { :index "default" :query (has-word-starting-with? "description" "stup")}))))))

     (it "will do exact matching on integers"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= "anteater" 
          (extract-name-from-result 
            (query/execute db engine { :index "default" :query (=? "number" 50) }) )))))

     (it "will do less than matching on integers"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= "zebra" 
          (extract-name-from-result 
            (query/execute db engine { :index "default" :query (<? "number" 30) }) )))))

      (it "will do greater than matching on integers"
        (with-full-setup
        (fn [db engine]
          (add-standard-data-set db)
          (indexing/wait-for-index-catch-up db 50)
          (should= "aardvark" 
            (extract-name-from-result 
              (query/execute db engine { :index "default" :query (>? "number" 499) }) )))))

     (it "will exclude the literal from the less than range"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= 1 
          (count (query/execute db engine { :index "default" :query (<? "number" 50) }))))))

      (it "will exclude the literal from the greater than range "
        (with-full-setup
        (fn [db engine]
          (add-standard-data-set db)
          (indexing/wait-for-index-catch-up db 50)
          (should= 1
            (count (query/execute db engine { :index "default" :query (>? "number" 100) }) )))))

      (it "will do less than or equal than matching on integers"
        (with-full-setup
          (fn [db engine]
            (add-standard-data-set db)
            (indexing/wait-for-index-catch-up db 50)
            (should= "zebra" 
              (extract-name-from-result 
                (query/execute db engine { :index "default" :query (<=? "number" 25) }) )))))

        (it "will do greater than or equal than matching on integers"
         (with-full-setup
          (fn [db engine]
            (add-standard-data-set db)
            (indexing/wait-for-index-catch-up db 50)
            (should= "aardvark" 
              (extract-name-from-result 
                (query/execute db engine { :index "default" :query (>=? "number" 500) }) )))))

      (it "will allow queries against symbol based keys"
        (with-full-setup
          (fn [db engine]
            (add-standard-data-set db)
            (indexing/wait-for-index-catch-up db 50)
            (should== { :name "Air" :album "Talkie Walkie" } 
              (read-string (first 
                (query/execute db engine { :index "default" :query (=? :name "Air") }) ))))))

      (it "will allow queries inside collections"
        (with-full-setup
          (fn [db engine]
            (with-open [tx (s/ensure-transaction db)]
              (-> tx
                (docs/store-document "1" (pr-str { "name" "bob" :collection [ "one" "two" "three"]}))
                (docs/store-document "2" (pr-str { "name" "alice" :collection [ "two" "three" "four"]}))
                (s/commit!)))
            (indexing/wait-for-index-catch-up db 50)
            (should= "bob"
              (extract-name-from-result
                (query/execute db engine { :index "default" :query (has-item? :collection "one")}))))))

      (it "will allow multiple claused joined by an 'and'"
        (with-full-setup
          (fn [db engine]
            (with-open [tx (s/ensure-transaction db)]
              (-> tx
                (docs/store-document "1" (pr-str { "name" "bob" :collection [ "one" "two" "three"]}))
                (docs/store-document "2" (pr-str { "name" "alice" :collection [ "two" "three" "four"]}))
                (docs/store-document "3" (pr-str { "name" "alice" :collection [ "one" "four"]}))
                (s/commit!)))
            (indexing/wait-for-index-catch-up db 50)
            (should== { "name" "alice" :collection [ "two" "three" "four"]}
              (read-string (first 
                  (query/execute db engine { :index "default" :query (AND (=? "name" "alice") (has-item? :collection "two")) })))))))  

      (it "will allow multiple claused joined by an 'or'"
        (with-full-setup
          (fn [db engine]
            (with-open [tx (s/ensure-transaction db)]
              (-> tx
                (docs/store-document "1" (pr-str { "name" "bob" :collection [ "one" "two" "three"]}))
                (docs/store-document "2" (pr-str { "name" "alice" :collection [ "two" "three" "four"]}))
                (docs/store-document "3" (pr-str { "name" "alice" :collection [ "one" "four"]}))
                (s/commit!)))
            (indexing/wait-for-index-catch-up db 50)
            (should= 3
              (count (query/execute db engine { :index "default" :query (OR (has-item? :collection "four") (has-item? :collection "two")) })))))) 

)

