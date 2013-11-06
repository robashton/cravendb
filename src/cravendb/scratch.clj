(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.tools.logging :only (info debug error)]
        [clojure.data.codec.base64]
        [clojure.core.async]))

(defn create-engine [] 
  {
   :command-channel (chan)
   :event-loop (atom nil)
  })


(defn schedule-removal [state index]
  (update-in state [:pending-removal] conj index))

(defn go-index-some-stuff [state]
  (go 
    (info "indexing stuff")
    (Thread/sleep 2000)
    (info "done indexing stuff")
    (>! (:command-channel state) { :cmd :notify-finished-indexing})))

(defn go-catch-up [index state]
  (go
    (info "running an index catch-up operation")
    (Thread/sleep 2500)
    (info "index is caught up")))

(defn main-indexing-process [state]
  (if (:indexing-channel state)
    (do (info "ignoring this request yo") state)
    (assoc state :indexing-channel (go-index-some-stuff state))))

(defn remove-dead-indexes [state]
    ;; Close the writers for these indexes
    ;; Dissoc them from the state
    ;; Great Success
  )

(defn add-caught-up-indexes [state]
  ;; Not sure how I'm going to synchronise this)
  ;; Maybe a "close enough" approach
  state)

(defn create-chaser [state index]
  (update-in state [:chasers] conj (go-catch-up state index)))

(defn main-indexing-process-ended [state]
  (-> state
    (dissoc :indexing-channel)
    (remove-dead-indexes)
    (add-caught-up-indexes)))

(defn be-prepared [_ {:keys [command-channel] :as engine}]
  (go (loop [state engine]
    (if-let [{:keys [cmd data]} (<! command-channel)]
     (do
       (recur (case cmd
         :schedule-indexing (main-indexing-process state)
         :notify-finished-indexing (main-indexing-process-ended state)
         :new-index (create-chaser engine state)
         :removed-index (schedule-removal state data))))
      (do
        (info "waiting for main index process")
        (if-let [main-indexing (:indexing-channel state)]
          (<!! (:indexing-channel state)))
        ;; Definitely need a way to cancel these, even if it's a global atom
        (info "waiting for chasers")
        (if-let [chasers (:chasers state)]
          (doseq [c chasers]
            (<!! c)))
        (info "I would be closing resources here"))))))

(defn start [{:keys [event-loop] :as engine}]
  (swap! event-loop #(be-prepared %1 engine))) 

(defn stop 
  [{:keys [command-channel event-loop]}]
  (close! command-channel)
  (<!! @event-loop)
  (info "finished doing everything"))

#_ (def engine (create-engine))
#_ (start engine)
#_ (stop engine)

#_ (do
     (go (>! (:command-channel engine) {:cmd :schedule-indexing}))
     (println "sent"))
