(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.tools.logging :only (info debug error)]
        [clojure.data.codec.base64]
        [clojure.core.async]))

(defn create-engine [] (atom {}))

(defn be-prepared [handle]
  (go (loop []
    (if-let [{:keys [cmd data]} (<! (:channel @handle))]
     (do
       (case cmd
         :new-index nil
         :removed-index nil
         )
       (recur))  
      (do
        ;; Close stuff and wait
        (Thread/sleep 1000)
        (info "closing")
        )
      ))))

(defn start [handle]
  (swap! handle #(assoc %1 
    :channel (chan)
    :main (be-prepared handle))))

(defn stop [handle]
  (close! (:channel @handle))
  (<!! (:main @handle))
  (info "finished"))

(def engine (create-engine))
#_ (start engine)
#_ (stop engine)
