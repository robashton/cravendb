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
            [cravendb.client :as client]
            [cravendb.replication :as r]
            [clojure.pprint :refer [pprint]]))

#_ (with-test-server 
      (fn []
        (client/put-index 
          "http://localhost:9000" 
          "by_username" 
          "(fn [doc] {\"username\" (:username doc)})")
        (client/put-document 
          "http://localhost:9000" 
          "1" { :username "bob"})
        (client/put-document 
          "http://localhost:9000" 
          "2" { :username "alice"})
        (client/query 
          "http://localhost:9000" 
          { :query "(= \"username\" \"bob\")" :index "by_username" :wait true}))) 

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

;; Bulk operations need to check history for conflicts
;; Replication needs to check history for conflicts
;; 

#_ (def tx (assoc (s/ensure-transaction (:storage instance))
              :e-id "root-1" 
               :base-vclock (:base-vclock instance)
               :last-synctag (:last-synctag instance)
               :server-id "root"))

#_ (pprint (r/replicate-into tx [
                      { :id "doc-1" :doc { :foo "bar" } :metadata { :synctag (integer-to-synctag 0)}}
                      { :id "doc-2" :doc { :foo "bar" } :metadata { :synctag (integer-to-synctag 1)}}
                      { :id "doc-3" :doc { :foo "bar" } :metadata { :synctag (integer-to-synctag 2)}}
                      { :id "doc-4" :doc { :foo "bar" } :metadata { :synctag (integer-to-synctag 3)}}
                      { :id "doc-5" :doc { :foo "bar" } :metadata { :synctag (integer-to-synctag 4)}}
                      ]))


;; Conflict scenarios
;; Document doesn't exist yet -> write away!
#_ (r/conflict-status
  nil (v/next "1" (v/new)))

;; Document exists, history is in the past -> write away!
#_ (r/conflict-status
  (v/next "1" (v/new)) (v/next "1" (v/next "1" (v/new))))

;; Document exists, history is in the future -> drop it!
#_ (r/conflict-status
    (v/next "1" (v/next "1" (v/new))) (v/next "1" (v/new)))

;; Document exists, history is conflicting -> conflict
#_ (r/conflict-status
    (v/next "1" (v/next "1" (v/new))) (v/next "2" (v/next "1" (v/new))))



