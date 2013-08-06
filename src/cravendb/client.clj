(ns cravendb.client
  (:require [http.async.client :as http]))

(defn url-for-id [url id]
  (str url "/doc/" id))

(defn load-document [url id]
  (with-open [client (http/create-client)]
    (let [response (http/GET client (url-for-id url id))]
      (-> response
          http/await
          http/string))))

(defn put-document [url id doc]
  (with-open [client (http/create-client)]
    (let [response (http/PUT client (url-for-id url id) :body doc)]
      (-> response
          http/await
          http/string))))

(defn delete-document [url id]
  (with-open [client (http/create-client)]
    (let [response (http/DELETE client (url-for-id url id))]
      (-> response
          http/await
          http/string))))
  



