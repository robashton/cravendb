(defproject cravendb "0.0.0"
  :min-lein-version "2.2.0"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clj-http "0.7.6"]
                 [ring "1.2.0"]
                 [compojure "1.1.5"]
                 [org.clojure/tools.nrepl "0.2.3"]
                 [org.fusesource.leveldbjni/leveldbjni-all "1.7"]
                 [me.raynes/fs "1.4.4"]
                 [http.async.client "0.5.2"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.slf4j/slf4j-log4j12 "1.6.6"]]

  :profiles {:dev {
                   :dependencies [[speclj "2.7.0"]
                                  [speclj-growl "2.1.0"]]}}
  :plugins [[speclj "2.5.0"]]
  :test-paths ["spec/"])

