(ns admin.core
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [goog.dom :as dom]
            [goog.events :as events]
            [cljs.core.async :refer [put! chan <! alts! timeout]]
            [admin.recent-documents :as rd]
            [admin.http :as http]))

(defn listen [el type]
  (let [out (chan)]
    (events/listen el type
      (fn [e] (put! out e)))
    out))

#_ (rd/stream-into (dom/getElement "recent-documents"))


(defn update-chart [items]
 (.log js/console (clj->js items)))

(go (let [out (http/longpoll "/stats")]
      (loop [history ()]
        (if-let [data (<! out)]
          (let [current (take 1000 (conj history data))]
            (update-chart current)
            (recur current))))))
