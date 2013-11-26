(ns cravendb.querylanguage)

(defn parse-element [k]
  (cond (string? k) (str "\"" k "\"")
        :else (str k)))

(defn binary-condition [n k v]
  (str "(" n " " (parse-element k) " " (parse-element v) ")"))


(defn list-condition [n cs]
  (str "(" n (reduce str (map (partial str " ") cs)) ")"))


(defn >? [k v] (binary-condition ">" k v))
(defn <? [k v] (binary-condition "<" k v)) 
(defn =? [k v] (binary-condition "=" k v)) 
(defn >=? [k v] (binary-condition ">=" k v)) 
(defn <=? [k v] (binary-condition "<=" k v)) 
(defn starts-with? [k v] (binary-condition "starts-with" k v)) 
(defn has-word? [k v] (binary-condition "has-word" k v))
(defn has-word-starting-with? [k v] (binary-condition "has-word-starting-with" k v))
(defn has-item? [k v] (binary-condition "=" k v))  ;; for now
(defn AND [& v] (list-condition "and" v))
(defn OR [& v] (list-condition "or" v))
(defn NOT [& v] (list-condition "not" v))


