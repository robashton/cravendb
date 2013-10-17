(ns cravendb.client
  (:require [http.async.client :as http])
  (:require [cemerick.url :refer (url-encode)] 
            [clojure.edn :as edn]
            [cravendb.core :refer [zero-etag]]
            [clojure.tools.logging :refer [debug info error]]
            ))

(defn url-for-doc-id [url id]
  (str url "/document/" id))

(defn url-for-index-id [url id]
  (str url "/index/" id))

(defn url-for-bulk-ops [url]
  (str url "/bulk"))

(defn url-for-stream [url etag]
  (str url "/stream?etag=" (or etag "")))

(defn url-for-query [url opts]
  (str 
    url "/query/"
    (opts :index) "/"
    (url-encode (opts :query))
    (if (opts :wait) "?wait=true")))

(defn to-db [data]
  (pr-str data))

(defn from-db [data]
  (if (nil? data) nil
    (edn/read-string data)))

(defn force-into-list [result]
  (if (nil? result) ()
   (if (seq? result) result (seq [result]))))

(defn from-http [input]
  (if (= 404 (:code @(:status input)))
    nil
    (from-db (http/string input))))

(defn process-response [response]
  (-> response
      http/await
      from-http))

(def default-headers { :accept "application/edn" })

(defn get-document [url id]
  (with-open [client (http/create-client)]
    (process-response 
      (http/GET client (url-for-doc-id url id) :headers default-headers))))

(defn put-document [url id doc]
  (with-open [client (http/create-client)]
    (process-response 
      (http/PUT client (url-for-doc-id url id) :body (to-db doc) :headers default-headers))))

(defn delete-document [url id]
  (with-open [client (http/create-client)]
    (process-response 
      (http/DELETE client (url-for-doc-id url id) :headers default-headers))))

(defn put-index [url id mapf]
  (with-open [client (http/create-client)]
    (process-response 
      (http/PUT client (url-for-index-id url id) :body (to-db {:map mapf}) :headers default-headers))))

(defn bulk-operation [url ops]
  (with-open [client (http/create-client)]
    (process-response 
      (http/POST client (url-for-bulk-ops url) :body (to-db ops) :headers default-headers))))

(defn get-index [url id]
  (with-open [client (http/create-client)]
    (process-response 
      (http/GET client (url-for-index-id url id) :headers default-headers))))

(defn query [url opts]
  (with-open [client (http/create-client)]
    (force-into-list
      (process-response
        (http/GET client (url-for-query url opts) :headers default-headers)))))

(defn stream 
  ([url] (stream url nil))
  ([url from-etag]
    (debug "Requesting" url from-etag)
    (with-open [client (http/create-client)]
      (force-into-list
        (process-response
          (http/GET client (url-for-stream url from-etag) :headers default-headers))))))

(defn stream-seq 
  ([url] (stream-seq url (zero-etag)))
  ([url etag] (stream-seq url etag (stream url etag)))
  ([url last-etag src]
   (if (empty? src) 
     (do
       (let [next-src (stream url last-etag)]
         (if (empty? next-src) ()
           (stream-seq url last-etag next-src))))
     (let [{:keys [metadata doc] :as item} (first src)]
       (cons item (lazy-seq (stream-seq url (:etag metadata) (rest src))))))))

