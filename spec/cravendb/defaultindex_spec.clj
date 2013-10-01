(ns cravendb.defaultindex-spec
  (:use [speclj.core]
        [cravendb.testing]
        [cravendb.core])
  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as s]
            [cravendb.client :as client]
            [cravendb.query :as query]
            [cravendb.lucene :as lucene]))

(defn add-standard-data-set [db]
  (with-open [tx (s/ensure-transaction db)] 
    (-> tx
      (docs/store-document "docs-1" (pr-str { "name" "zebra" "description" "the zebra is a horse like creature with black and white stripes" "number" 25}))
      (docs/store-document "docs-2" (pr-str { "name" "aardvark" "description" "the aardvaark has a lot of a's in his name, kinda silly really" "number" 500}))
      (docs/store-document "docs-3" (pr-str { "name" "giraffe" "description" "the giraffe looks a bit silly with its big long neck" "number" 100}))
      (docs/store-document "docs-4" (pr-str { "name" "anteater" "description" "the anteater has a stupid looking long nose" "number" 50}))
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
            (query/execute db engine { :index "default" :query "(= \"name\" \"zebra\")" }))))))
   (it "will search on partial short strings"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= "zebra" 
          (extract-name-from-result 
            (query/execute db engine { :index "default" :query "(starts-with \"name\" \"zeb\")" }))))))
    (it "will do word-based matching on long strings"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= "zebra" 
          (extract-name-from-result 
            (query/execute db engine { :index "default" :query "(= \"description\" \"horse\")" }) )))))
          
    (it "will do partial-word-based matching on long strings"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= "anteater" 
          (extract-name-from-result 
            (query/execute db engine { :index "default" :query "(starts-with \"description\" \"stup\")" }))))))

     (it "will do exact matching on integers"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= "anteater" 
          (extract-name-from-result 
            (query/execute db engine { :index "default" :query "(= \"number\" 50)" }) )))))

     (it "will do less than matching on integers"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= "zebra" 
          (extract-name-from-result 
            (query/execute db engine { :index "default" :query "(< \"number\" 30)" }) )))))

      (it "will do greater than matching on integers"
        (with-full-setup
        (fn [db engine]
          (add-standard-data-set db)
          (indexing/wait-for-index-catch-up db 50)
          (should= "aardvark" 
            (extract-name-from-result 
              (query/execute db engine { :index "default" :query "(> \"number\" 499)" }) )))))

     (it "will exclude the literal from the less than range"
     (with-full-setup
      (fn [db engine]
        (add-standard-data-set db)
        (indexing/wait-for-index-catch-up db 50)
        (should= 1 
          (count (query/execute db engine { :index "default" :query "(< \"number\" 50)" }))))))

      (it "will exclude the literal from the greater than range "
        (with-full-setup
        (fn [db engine]
          (add-standard-data-set db)
          (indexing/wait-for-index-catch-up db 50)
          (should= 1
            (count (query/execute db engine { :index "default" :query "(> \"number\" 100)" }) )))))

      (it "will do less than or equal than matching on integers"
        (with-full-setup
          (fn [db engine]
            (add-standard-data-set db)
            (indexing/wait-for-index-catch-up db 50)
            (should= "zebra" 
              (extract-name-from-result 
                (query/execute db engine { :index "default" :query "(<= \"number\" 25)" }) )))))

        (it "will do greater than or equal than matching on integers"
         (with-full-setup
          (fn [db engine]
            (add-standard-data-set db)
            (indexing/wait-for-index-catch-up db 50)
            (should= "aardvark" 
              (extract-name-from-result 
                (query/execute db engine { :index "default" :query "(>= \"number\" 500)" }) )))))

          )

