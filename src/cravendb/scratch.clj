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
