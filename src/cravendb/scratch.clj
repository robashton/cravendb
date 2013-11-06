(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.tools.logging :only (info debug error)]
        [clojure.data.codec.base64]
        [clojure.core.async]))

(defn create-engine [] (atom {}))

(defn main-loop [engine]
  (while (:running @engine)
    (info "main loop running")
    (Thread/sleep 100)))

(defn start [engine]
  (swap! engine #(assoc %1 
    :running true
    :main (go (main-loop engine)))))

(defn stop [engine]
  (swap! engine #(assoc %1 :running false))
  (<!! (:main @engine))
  (info "finished"))

(def engine (create-engine))
#_ (start engine)
#_ (stop engine)

(def pending-catch-ups (chan 100))
(def finished-catch-ups (chan 100))

  ;; And this will add the catch-up back to the ordinary list
  (go 
    (while true
     (let [finished (<! finished-catch-ups)]
       )))

  ;; So this fucker will listen for desired catch-ups forever
  (go 
    (while true
    (let [catch-up (<! pending-catch-ups)]
      (println "starting" catch-up)
      (Thread/sleep (rand 1000))
      (println "finishing" catch-up)
      (>! finished-catch-ups catch-up))))

  (<!! (go  
    (<! (go (>! pending-catch-ups { :id "1"} )
        (println "sent 1")))
    (<! (go (>! pending-catch-ups { :id "2"} )
        (println "sent 2")))
    (<! (go (>! pending-catch-ups { :id "3"} )
        (println "sent 3")))))

    (Thread/sleep 5000))

#_ (let [c1 (chan)
       c2 (chan)]
  (go (while true
        (let [v (<! c1)]
          (println "read" v "from"))))
  (go (>! c1 "hi"))
  (go (>! c2 "there"))
     
     (Thread/sleep 100)
     )

#_ (<!! 
     (go
       (println "starting")
       (Thread/sleep 1000)
       (println "ending")
     ))


