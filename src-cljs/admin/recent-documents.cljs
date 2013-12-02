(ns admin.recent-documents
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [goog.dom :as dom]
            [goog.events :as events]
            [cljs.core.async :refer [put! chan <! alts! timeout]]
            [admin.http :as http]))

(defn documents-from-synctag [synctag]
  (http/request "GET" (str "/stream?synctag=" synctag)))

(defn stream-from [synctag]
  (let [out (chan) ]
    (go (loop [result (<! (documents-from-synctag synctag))]
          (doseq [v (:body result)] (put! out v))
          (<! (timeout 200))
          (recur (<! (documents-from-synctag (get-in result [:headers :last-synctag]))))))
    out))

(defn render [docs]
  (str 
    "<table class='table table-striped'><thead><tr>"
      "<td>Synctag</td>"
      "<td>Id</td>"
    "</tr></thead>"
    (apply str
    (for [doc docs]
      (do
        (str 
        "<tr>"
          "<td>" (get-in doc [:metadata :synctag]) "</td>"
          "<td>" (:id doc) "</td>"
        "</tr>"
        ))
      ))
    "</table>"))

(defn stream-into [el]
  (let [in (stream-from "000000000000000000000000000000")] 
    (go (loop [docs ()]
          (set! (.-innerHTML el) (render docs))
          (recur (take 10 (cons (<! in) docs)))))))


