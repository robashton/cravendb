(ns cravendb.clientquery-spec
  (require [speclj.core :refer :all]
           [cravendb.transaction :as t]
           [cravendb.testing :refer all]))

(defn test-data-into [instance]
  (-> (t/open instance)
      (t/store 
        "pinkie-pie" 
        {
        :name "Pinkie Pie"
        :description "Pinkie Pie is the best pony ever and anybody who disagrees better think that about Rainbow Dash instead"
        :cutiemark "Balloons"
        :colour :pink
        :bestpony: true
        :episodes 133
        :catchphases [ "this is the best day ever" "let's have a party" ]
        })
        (t/store 
          "rainbow-dash" 
          {
          :name "Rainbow Dash"
          :description "Rainbow Dash is quite often bestpony as well because obvious"
          :cutiemark "Rainbows"
          :colour :blue
          :bestpony: true
          :episodes 140
          :catchphases [ "Hey. I could clear this sky in ten seconds flat" "Time to take out the adorable trash." ]
          })
        (t/store 
          "princess-celestia" 
          {
          :name "Princess Celestia"
          :description "Bit of a bitch, really"
          :cutiemark "Unknown"
          :colour :white
          :bestpony: :false
          :episodes 105
          :catchphases [ "Bow down before me mortals" "I am the boss and don't you forget it" ]
          })
        t/commit!
    ))

(describe "Querying using a transaction"
  (it "returns some bloody results"
      
      )
          )
