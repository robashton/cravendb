(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:require [cravendb.database :as db]
            [cravendb.transaction :as t]
            [cravendb.testing :refer :all]
            [cravendb.embedded :as embedded]
            [cravendb.counters :as counters]
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

#_ (http/send-data { :foo "bar"})

#_ (counters/append (:counters embedded-instance) :foo)
