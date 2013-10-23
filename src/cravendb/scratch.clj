(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.data.codec.base64])
  (:require [cravendb.vclock :as v]
            [cravendb.documents :as docs]
            [clojurewerkz.vclock.core :as vclock]            
            [clojure.edn :as edn]
            [cravendb.database :as db]
            [cravendb.storage :as s]
            [me.raynes.fs :as fs]
            ))

;; I think I should rename synctags, database-specific incrementor
;; - Used for indexing location
;; - Used for replication location
;; - the "history" for an item

;; The server should assign client-ids
;; When an in-flight transaction starts
;; It should be a combination of the server id and some integer
;; I can code that up in the REPL

(defn start []
  (def instance (db/create "testdb")))

(defn stop []
  (.close instance))

(defn restart []
  (stop)
  (fs/delete-dir "testdb")
  (start))

#_ (start)
#_ (stop)
#_ (restart)



#_ (db/checked-history {:server-id "2"} "doc-1"nil nil) 
#_ (db/put-document instance "doc-1" {:name "bob"})
#_ (db/load-document instance "doc-1" )
#_ (v/string-to-vclock (:history (db/load-document-metadata instance "doc-1")))

#_ (db/put-document instance "doc-1" {:name "bob"}
   (db/in-tx instance (fn [tx]
                        (db/load-document-metadata tx "doc-1")))) 

#_ (next-vclock "1" (vclock/fresh) nil)
#_ (next-vclock "1" (vclock/fresh) 
                (vclock-to-string (vclock/increment (vclock/fresh) "2")))


; Writing a new document
#_ (db/checked-history { :server-id "one" :base-vclock (v/new )} nil nil)

;; Writing a document without specifying a history
#_ (db/checked-history { :server-id "one" :base-vclock (v/new )} nil (v/next "one" (v/new)))

;; Writing a document specifying an invalid

#_ (db/checked-history { :server-id "one" :base-vclock (v/new )} 
                       (v/next "two" (v/new)) (v/next "one" (v/new)))


#_ (s/get-string instance "doc-doc-1")

#_ (v/descends? 
     (v/next "one" (v/next "two" (v/new)))
     (v/next "one" (v/new))
     )


#_ (vclock/descends?
     (vclock/increment (vclock/increment (vclock/increment (vclock/fresh) "three") "two") "one")
     (vclock/increment (vclock/increment (vclock/fresh) "three") "one"))

#_ (with-full-setup 
  (fn [{:keys [storage] :as instance}]
    (db/put-document instance "1" "hello world")
    (let [old-meta (docs/load-document-metadata storage "1")]
      (db/put-document instance "1" "hello world")   
      (db/put-document instance "1" "hello bob" old-meta))
    (docs/conflicts storage)))

