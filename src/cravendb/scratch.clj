(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:require [cravendb.database :as db]
            [cravendb.transaction :as t]
            [cravendb.testing :refer :all]
            [cravendb.embedded :as embedded]
            [org.httpkit.server :refer [run-server]]
            [cravendb.http :as http]
            [cravendb.transaction :as t]
            [cravendb.querylanguage :refer :all]
            [cravendb.client :as client]))

#_ (def instance (embedded/create))
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


