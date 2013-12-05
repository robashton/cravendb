(ns admin.http
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [clojure.string :as s]
            [cljs.core.async :refer [put! chan <! alts! timeout]]
            [cljs.reader :as reader])
  (:import [goog.net Jsonp]
           [goog Uri]))

(defn as-symbol [k]
  (keyword (s/replace (s/lower-case k) #" " "-")))

(defn headers-map [headers] (into {} (->>
           (s/split headers #"\n") 
           (map (fn [v] (s/split v #":" 2))) 
           (map (fn [[k v]] [(as-symbol k) (s/trim v)])))))

(defn parsed-body [res]
  (if (empty? res) nil (reader/read-string res)))

(defn parsed-response [req]
  (let [out (chan)]
    (go (put! out {:body (parsed-body (. req -response))
                   :status (. req -status)
                   :status-text (. req -statusText)
                   :headers (headers-map (.getAllResponseHeaders req))
                   }))out))

(defn http-request-ready-change [req out]
  (fn [v]
    (go (let [ready-state (. req -readyState)]
        (if (= 4 ready-state) (put! out (<! (parsed-response req))))))))

(defn request [request-type url]
  (let [out (chan)
        req (js/XMLHttpRequest.)]
    (aset req "onreadystatechange" (http-request-ready-change req out)  )
    (.open req request-type url)
    (.setRequestHeader req "Content-Type" "application/edn" )
    (.setRequestHeader req "Accept" "application/edn" )
    (.send req)
    out))



