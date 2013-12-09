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

(def chart (atom nil))
(def seconds (atom 0))
(defn charty-chart 
  []
  (swap! chart 
         (fn [v]
           (if v v
             (let [svg (dimple.newSvg. "#indexing-activity" 590 400)
                   chart (dimple.chart. svg [])]
               (.addOrderRule 
                 (.addCategoryAxis chart "x" (clj->js ["second" "metric"])) "second")
               (.addMeasureAxis chart "y" "amount")
               (.addLegend chart 65, 10, 510, 20, "right");
               (.addSeries chart "metric" dimple.plot.bar)
               chart)))))

(defn update-chart [items]
 (let [chart (charty-chart)]
   (aset chart "data" (clj->js items))
   (.draw chart)))

(defn munge-data [data]
  (let [sec (swap! seconds inc)]
    [ {
       "second" sec
       "metric" "docs added"
       "amount" (:doc-added data)
       }
     {
      "second" sec
      "metric" "docs indexed"
      "amount" (:indexed data)
      } ]))

(go (let [out (http/longpoll "/stats")]
      (loop [history ()]
        (if-let [data (<! out)]
          (let [current (take 1000 (apply conj history (munge-data data)))]
            (update-chart current)
            (recur current))))))

