(ns admin.core
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [goog.dom :as dom]
            [goog.events :as events]
            [cljs.core.async :refer [put! chan <! alts!]])
  (:import [goog.net Jsonp]
           [goog Uri]))

(defn http-request-ready-change [req out]
  (fn [v]
    (go (let [ready-state (. req -readyState)]
        (if (= 4 ready-state) (put! out req))))))

(defn http-request [request-type url]
  (let [out (chan)
        req (js/XMLHttpRequest.)]
    (aset req "onreadystatechange" (http-request-ready-change req out)  )
    (.open req request-type url)
    (.setRequestHeader req "Content-Type" "application/edn" )
    (.setRequestHeader req "Accept" "application/edn" )
    (.send req)
    out))

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

(go (.log js/console (<! (http-request "GET" "/document/pinkie"))))

#_ (go (.log js/console (<! (jsonp (search-url "cats")))))
