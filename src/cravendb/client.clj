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

(defn process-response [response]
  (-> response
      http/await
      http/string
      from-db))

(defn load-document [url id]
  (with-open [client (http/create-client)]
    (process-response (http/GET client (url-for-id url id)))))

(defn put-document [url id doc]
  (with-open [client (http/create-client)]
    (process-response (http/PUT client (url-for-id url id) :body (to-db doc)))))

(defn delete-document [url id]
  (with-open [client (http/create-client)]
    (process-response (http/DELETE client (url-for-id url id)))))
  




