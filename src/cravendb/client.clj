(ns cravendb.client
  (require [http.async.client :as http]))

(defn url-for-doc-id [url id]
  (str url "/doc/" id))

(defn url-for-index-id [url id]
  (str url "/index/" id))

(defn url-for-query [url opts]
  (str 
    url "/query/"
    (opts :index) "/"
    (opts :query)
    (if (opts :wait) "?wait=true")))

(defn to-db [data]
  (pr-str data))

(defn from-db [data]
  (if (nil? data)
    nil
    (read-string data)))

(defn force-into-list [result]
  (if (seq? result) result (seq [result])))

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

(defn query [url opts]
  (with-open [client (http/create-client)]
    (force-into-list
      (process-response
        (http/GET client (url-for-query url opts))))))
