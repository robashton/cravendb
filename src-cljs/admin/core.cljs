(ns admin.core
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [goog.dom :as dom]
            [goog.events :as events]
            [cljs.core.async :refer [put! chan <! alts!]])
  (:import [goog.net Jsonp]
           [goog Uri]))


#_ (cemerick.piggieback/cljs-repl :repl-env (cemerick.austin/exec-env))

(defn listen [el type]
  (let [out (chan)]
    (events/listen el type
      (fn [e] (put! out e)))
    out))

(defn jsonp [uri]
  (let [out (chan) req (Jsonp. (Uri. uri))]
    (.send req nil (fn [e] (put! out e)))
    out))

(def wiki-search-url "http://en.wikipedia.org/w/api.php?action=opensearch&format=json&search=")

(defn search-url [q]
  (str wiki-search-url q))

(let [clicks (listen (dom/getElement "search") "click")]
  (go (while true
        (.log js/console (<! clicks)))))
(go (.log js/console (<! (jsonp (search-url "cats")))))
