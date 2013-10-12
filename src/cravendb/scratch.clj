(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        )

  (:require [cravendb.indexing :as indexing]
            [cravendb.documents :as docs]
            [me.raynes.fs :as fs]
            [cravendb.indexstore :as indexes]
            [cravendb.indexengine :as indexengine]
            [cravendb.storage :as s]
            [cravendb.client :as client]
            [cravendb.query :as query]
            [cravendb.database :as db]
            [cravendb.lucene :as lucene]))


(def test-index 
  "(fn [doc] (if (:whatever doc) { \"whatever\" (:whatever doc) } nil ))")

(defn add-by-whatever-index [instance]
  (db/put-index instance { 
                         :id "by_whatever" 
                         :map test-index})) 

(defn add-1000-documents [instance]
  (db/bulk instance
    (map (fn [i]
           {
            :operation :docs-put
            :id (str "docs-" i)
            :document { :whatever (str "docblah" i)} 
            }) (range 0 1000))))


(defn add-alpha-whatevers [instance]
  (db/put-document instance "docs-1" (pr-str { :whatever "zebra"}))
  (db/put-document instance "docs-2" (pr-str { :whatever "aardvark"}))
  (db/put-document instance "docs-3" (pr-str { :whatever "giraffe"}))
  (db/put-document instance "docs-4" (pr-str { :whatever "anteater"})))

#_ (fs/delete-dir "testdb")
#_ (def instance (db/create "testdb"))
#_ (add-by-whatever-index instance)
#_ (add-alpha-whatevers instance)
#_ (add-1000-documents instance)
#_ (db/query instance
                    { :query "*" :amount 100 :offset 0 :index "by_whatever"})

#_ (.close instance)



#_ (describe "paging like a boss"
  (it "will return the first 10 docs"
    (with-full-setup
    (fn [{:keys [storage index-engine] :as instance}]
      (add-by-whatever-index instance) 
      (add-1000-documents instance)
      (indexing/wait-for-index-catch-up storage 50)
      (should== (map str (range 0 10)) 
                (map (comp :whatever read-string) 
                     (database/query instance
                      { :query "*" :amount 10 :offset 0 :index "by_whatever"}))))))

   (it "will return the last 5 docs"
    (with-full-setup
    (fn [{:keys [storage index-engine] :as instance}]
      (add-by-whatever-index instance) 
      (add-1000-documents instance)
      (indexing/wait-for-index-catch-up storage 50)
      (should== (map str (range 995 1000)) 
                (map (comp :whatever read-string) 
                     (database/query instance 
                      { :query "*" :amount 10 :offset 995 :index "by_whatever"})))))))
