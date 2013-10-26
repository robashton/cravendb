(defproject cravendb "0.0.0"
  :min-lein-version "2.2.0"
  ;;:global-vars {*warn-on-reflection* true}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clj-http "0.7.6"]
                 [org.clojure/data.csv "0.1.2"] ;; For load  purposes
                 [com.cemerick/url "0.1.0"]
                 [liberator "0.9.0"]
                 [instaparse "1.2.2"]
                 [http-kit "2.1.12"]
                 [compojure "1.1.5"]
                 [serializable-fn "1.1.3"]
                 [clojurewerkz/vclock "1.0.0"]
                 [clj-time "0.6.0"]
                 [org.clojure/tools.nrepl "0.2.3"]
                 [org.fusesource.leveldbjni/leveldbjni-all "1.7"]
                 [me.raynes/fs "1.4.4"]
                 [http.async.client "0.5.2"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.slf4j/slf4j-log4j12 "1.6.6"]
                 [org.clojure/core.incubator "0.1.3"]
                 [org.clojure/core.async "0.1.0-SNAPSHOT"]
                 [org.apache.lucene/lucene-core "4.4.0"]
                 [org.apache.lucene/lucene-queryparser "4.4.0"]
                 [org.apache.lucene/lucene-analyzers-common "4.4.0"]
                 [org.clojure/data.codec "0.1.0"]
                 [spyscope "0.1.3"]
                 [org.apache.lucene/lucene-highlighter "4.4.0"]]

  :repositories {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}
  :profiles {:dev {
                   :dependencies [[speclj "2.7.2"]
                                  [speclj-growl "1.0.0-SNAPSHOT"]
                                  [speclj-growl "2.1.0"]]}
             }
  :injections [(require 'spyscope.core)]
  :plugins [[speclj "2.7.2"]]
  :main cravendb.http
  :test-paths ["spec/"])

