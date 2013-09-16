(ns cravendb.client
  (require [http.async.client :as http]))

(defn url-for-doc-id [url id]
  (str url "/doc/" id))

(defn url-for-index-id [url id]
  (str url "/index/" id))

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

(defn get-document [url id]
  (with-open [client (http/create-client)]
    (process-response 
      (http/GET client (url-for-doc-id url id)))))

(defn put-document [url id doc]
  (with-open [client (http/create-client)]
    (process-response 
      (http/PUT client (url-for-doc-id url id) :body (to-db doc)))))

(defn delete-document [url id]
  (with-open [client (http/create-client)]
    (process-response 
      (http/DELETE client (url-for-doc-id url id)))))

(defn put-index [url id mapf]
  (with-open [client (http/create-client)]
    (process-response 
      (http/PUT client (url-for-index-id url id) :body (to-db {:map mapf})))))

(defn get-index [url id]
  (with-open [client (http/create-client)]
    (process-response 
      (http/GET client (url-for-index-id url id)))))
