(ns cravendb.client
  (:require [http.async.client :as http]))

(defn url-for-id [url id]
  (str url "/doc/" id))

(defn to-db [data]
  (pr-str data))

(defn from-db [data]
  (if (nil? data)
    nil
    (read-string data)))

(defn load-document [url id]
  (with-open [client (http/create-client)]
    (let [response (http/GET client (url-for-id url id))]
      (-> response
          http/await
          http/string
          from-db))))

(defn put-document [url id doc]
  (with-open [client (http/create-client)]
    (let [response (http/PUT client (url-for-id url id) :body (to-db doc))]
      (-> response
          http/await
          http/string))))

(defn delete-document [url id]
  (with-open [client (http/create-client)]
    (let [response (http/DELETE client (url-for-id url id))]
      (-> response
          http/await
          http/string))))
  



