(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:require [cravendb.database :as db]
            [cravendb.transaction :as t]
            [cravendb.testing :refer :all]
            [cravendb.embedded :as embedded]
            [cravendb.remote :as remote]
            [org.httpkit.server :refer [run-server]]
            [cravendb.http :as http]
            [cravendb.transaction :as t]
            [cravendb.querylanguage :refer :all]
            [cravendb.client :as client]))

#_ (def server (run-server (http/create-http-server (embedded/create)) { :port 8001 :join? false }))
#_ (server)
#_ (def instance (remote/create :href "http://localhost:8001"))
#_ (.close instance)

#_ (-> (t/open instance)
     (t/store "pinkie" { :name "pinkie pie" :favourite-things [ "pies" "cakes" "flowers"]})
     (t/store "rainbow" { :name "Rainbow Dash" :favourite-things [ "rainbows" "clouds" "flowers"]})
     (t/store "derpy" { :name "Derpy Hooves" :favourite-things [ "muffins"]})
     (t/commit!))

#_ (db/query instance { :index "default" :filter (=? :name "pinkie pie") })
#_ (db/query instance { :index "default" :filter (starts-with? :name "pinkie") })
#_ (db/query instance { :index "default" :filter (starts-with? :name "pinkie") })


#_ (db/query instance { :index "default" :filter (has-item? :favourite-things "cakes") })


