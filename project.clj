(defproject cravendb "0.0.0"
  :min-lein-version "2.2.0"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clj-http "0.7.6"]
                 [ring "1.2.0"]
                 [compojure "1.1.5"]
                 [org.clojure/tools.nrepl "0.2.3"]
                 [org.fusesource.leveldbjni/leveldbjni-all "1.7"]]
  :main cravendb.server)
