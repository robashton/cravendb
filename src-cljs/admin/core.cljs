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

; (rd/stream-into (dom/getElement "recent-documents"))


(go (let [out (http/longpoll "/stats")]
      (loop []
        (if-let [data (<! out)]
          (do
            (.log js/console (clj->js data))
            (recur))))))
