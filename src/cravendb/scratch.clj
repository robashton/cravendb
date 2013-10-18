(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.data.codec.base64]
        )
  (:require [clojurewerkz.vclock.core :as vclock]
            [clojure.edn :as edn]))

;; Document one was created
#_ (def doc1 (vclock/increment (vclock/fresh) "docs-1"))

;; Document two was created
#_ (def doc2 (vclock/increment (vclock/fresh) "docs-2"))

;; I created a new version of document 1 
#_ (def doc1v2 (vclock/increment doc1 "docs-1"))

;; Document one is unrelated to document 2
#_ (vclock/descends? doc1 doc2)

;; Document one v2 descends from document one v1
#_ (vclock/descends? doc1v2 doc1)

#_ (println doc1)
#_ (println doc2)

;; We can encode to and from base64 (synctags)

(defn vclock-to-string [clock]
  (String. (encode (.getBytes (pr-str clock)))))

(defn string-to-vclock [in]
  (edn/read-string (println (String. (decode (.getBytes in))))))

;;But won't they get big?

(vclock-to-string (vclock/increment (vclock/increment (vclock/increment doc1v2 "docs-1") "docs-1") "docs-1"))

#_ (println encoded)

;; So I should prune them on write

;; I think I should rename synctags, database-specific incrementor
;; - Used for indexing location
;; - Used for replication location
;; - the "history" for an item

;; The server should assign client-ids
;; When an in-flight transaction starts
;; It should be a combination of the server id and some integer
;; I can code that up in the REPL











