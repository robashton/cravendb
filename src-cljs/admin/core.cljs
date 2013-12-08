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

(rd/stream-into (dom/getElement "recent-documents"))

(def chart (atom nil))
(def seconds (atom 0))
(defn charty-chart 
  []
  (swap! chart 
         (fn [v]
           (if v v
             (let [svg (dimple.newSvg. "#indexing-activity" 590 400)
                   chart (dimple.chart. svg [])]
               (.addCategoryAxis chart "x" "second")
               (.addCategoryAxis chart "y" "doc-added")
               (.addSeries chart nil dimple.plot.bar)
               chart)))))

(defn update-chart [items]
 (let [chart (charty-chart)]
   (aset chart "data" items)
   (.draw chart)))

(go (let [out (http/longpoll "/stats")]
      (loop [history ()]
        (if-let [data (<! out)]
          (let [current (take 1000 (conj history (assoc data "second" (swap! seconds inc))))]
            (update-chart current)
            (recur current))))))

