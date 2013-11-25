(ns cravendb.remote
  (:require [http.async.client :as http])
  (:require [cemerick.url :refer (url-encode)] 
            [clojure.edn :as edn]
            [cravendb.core :refer [zero-synctag]]
            [cravendb.database :refer [DocumentDatabase]]
            [clojure.tools.logging :refer [debug info error]]))

(defn url-for-doc-id [url id]
  (str url "/document/" id))
(defn url-for-index-id [url id]
  (str url "/index/" id))
(defn url-for-bulk-ops [url]
  (str url "/bulk"))
(defn url-for-conflicts [url]
  (str url "/conflicts"))
(defn url-for-conflict-id [url id]
  (str url "/conflict/" id))
(defn url-for-stream [url synctag]
  (str url "/stream?synctag=" (or synctag "")))

(defn url-for-query [url opts]
  (str 
    url "/query/"
    (opts :index) "/"
    (url-encode (opts :filter))
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

(defn interpret-headers [input]
  (let [headers (http/headers input)] 
    (edn/read-string (or (headers "cravendb-metadata") "{}"))))

(defn extract-headers [metadata]
  {  "cravendb-metadata" (pr-str metadata)
     "etag" (:history metadata) } )
   
(defn process-response [response]
  (-> response
      http/await
      from-http))

(defn read-metadata [response]
  (-> response
      http/await
      interpret-headers))

(def default-headers { :accept "application/edn" })

(defrecord RemoteDatabase [url]
  DocumentDatabase
  (close [this])
  (load-document-metadata [this id]
    (with-open [client (http/create-client)]
      (read-metadata 
        (http/GET client (url-for-doc-id url id) :headers default-headers))))

  (query [this opts]
    (with-open [client (http/create-client)]
      (force-into-list
        (process-response
          (http/GET client (url-for-query url opts) :headers default-headers)))))

  (clear-conflicts [this id]
    (with-open [client (http/create-client)]
      (process-response 
        (http/DELETE client (url-for-conflict-id url id) :headers default-headers))))

  (conflicts [this]
    (with-open [client (http/create-client)]
        (force-into-list
          (process-response
            (http/GET client (url-for-conflicts url) :headers default-headers)))))

  (put-document [this id document metadata]
    (with-open [client (http/create-client)]
    (process-response 
      (http/PUT client (url-for-doc-id url id) :body (to-db document) :headers (merge default-headers (extract-headers metadata))))))

  (delete-document [this id metadata]
    (with-open [client (http/create-client)]
      (process-response 
        (http/DELETE client (url-for-doc-id url id) :headers (merge default-headers (extract-headers metadata))))))

  (load-document [this id]
    (with-open [client (http/create-client)]
    (process-response 
      (http/GET client (url-for-doc-id url id) :headers default-headers)))) 

  (bulk [this operations]
    (with-open [client (http/create-client)]
    (process-response 
      (http/POST client (url-for-bulk-ops url) :body (to-db operations) :headers default-headers))))

  (put-index [this index]
    (with-open [client (http/create-client)]
    (process-response 
      (http/PUT client (url-for-index-id url (:id index)) :body (to-db index) 
                :headers default-headers))))

  (load-index-metadata [this id]
    
    )

  (delete-index [this id]
    (with-open [client (http/create-client)]
      (process-response 
        (http/DELETE client (url-for-index-id url id) :headers default-headers))))

  (load-index [this id]
    (with-open [client (http/create-client)]
      (process-response 
        (http/GET client (url-for-index-id url id) :headers default-headers)))))

(defn create [& kvs]
  (let [opts (apply hash-map kvs)]
   (RemoteDatabase. (:href opts))))
