(def mylist '(1 2 3))
(println mylist)
(println mymap)

(conj mylist 5)

(def mymap {
             :x {
                 "fred" "value"
                 }
             })

(assoc-in mymap [:x "fred"] "balls")
