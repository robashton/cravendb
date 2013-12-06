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


(defn longpoll 
  ([url] (let [out (chan)] (longpoll url out) out))
  ([url out]
   (let [req (js/XMLHttpRequest.)]
     (.open req "GET" url true) 
     (aset req "onreadystatechange" 
      (fn [] 
        (go 
          (let [readyState (. req -readyState) 
                status (. req -status)]
        (cond
          (and (= readyState 4) (= status 200)) (>! out (<! (parsed-response req)))
          (and (= readyState 4) (> 0 status)) (longpoll url out))
        (.send req)))))
     (.send req))))

;function longPoll() {
;
;  var xhr = createXHR(); // Creates an XmlHttpRequestObject
;  xhr.open('GET', 'LongPollServlet', true);
;  xhr.onreadystatechange = function () {
;    if (xhr.readyState == 4) {
;
;        if (xhr.status == 200) {
;            ...
;        }
;
;        if (xhr.status > 0) {
;            longPoll();
;        }
;    }
;  }
;  xhr.send(null);
;}
