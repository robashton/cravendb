(ns cravendb.querylanguage)

(defn parse-element [k]
  (cond (string? k) (str "\"" k "\"")
        :else (str k)))

(defn binary-condition [n k v]
  (str "(" n " " (parse-element k) " " (parse-element v) ")"))

(defn >? [k v] (binary-condition ">" k v))
(defn <? [k v] (binary-condition "<" k v)) 
(defn =? [k v] (binary-condition "=" k v)) 
(defn >=? [k v] (binary-condition ">=" k v)) 
(defn <=? [k v] (binary-condition "<=" k v)) 
(defn starts-with? [k v] (binary-condition "starts-with" k v)) 
(defn has-word? [k v] (binary-condition "=" k v))  ;; for now
(defn has-word-starting-with? [k v] (binary-condition "starts-with" k v))  ;; for now


