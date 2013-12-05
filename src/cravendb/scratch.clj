(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:require [cravendb.database :as db]
            [cravendb.transaction :as t]
            [cravendb.testing :refer :all]
            [cravendb.embedded :as embedded]
            [cravendb.remote :as remote]
            [org.httpkit.server :refer [run-server]]
            [clojure.core.async :refer [<! >! <!! put! chan go close! timeout ]]
            [cravendb.http :as http]
            [cravendb.transaction :as t]
            [cravendb.querylanguage :refer :all]
            [cravendb.client :as client]))

#_ (def embedded-instance (embedded/create))

#_ (def server (run-server (http/create-http-server embedded-instance) { :port 8001 :join? false }))
#_ (server)
#_ (def instance (remote/create :href "http://localhost:8001"))
#_ (.close instance)

#_ (-> (t/open instance)
     (t/store "pinkie" { :name "pinkie pie" :favourite-things [ "pies" "cakes" "flowers"]})
     (t/store "rainbow" { :name "Rainbow Dash" :favourite-things [ "rainbows" "clouds" "flowers"]}) (t/store "derpy" { :name "Derpy Hooves" :favourite-things [ "muffins"]})
     (t/commit!))

#_ (-> (t/open instance)
     (t/store "pinkie" { :name "pinkie pie lol" :favourite-things [ "pies" "cakes" "flowers"]})
     (t/store "rainbow" { :name "Rainbow Dash" :favourite-things [ "rainbows" "clouds" "flowers"]})
     (t/store "derpy" { :name "Derpy Hooves" :favourite-things [ "muffins"]})
     (t/commit!))

#_ (get-in embedded-instance [:storage :last-synctag])

#_ (db/query instance { :index "default" :filter (=? :name "pinkie pie") })
#_ (db/query instance { :index "default" :filter (starts-with? :name "pinkie") })
#_ (db/query instance { :index "default" :filter (starts-with? :name "pinkie") })


#_ (db/query instance { :index "default" :filter (has-item? :favourite-things "cakes") })

(def d1 [2 3 5])
(def d2 [200 100 50])

(defn avg1 []
  (/ (reduce + d1) (count d1)))

(defn avg2 []
  (/ (reduce + d2) (count d2)) )

(defn cumulative []
  (float (/ (+ (avg1) (avg2)) 2)))

(defn real []
  (let [all (concat d1 d2)]
      (/ (reduce + all) (count all))))


