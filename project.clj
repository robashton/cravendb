(defproject cravendb "0.1.0-SNAPSHOT"
  :description "A clojure-oriented document-oriented database"
  :url "http://robashton.github.io/cravendb"
  :min-lein-version "2.2.0"
  ;;:global-vars {*warn-on-reflection* true}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/core.async "0.1.256.0-1bf8cf-alpha"] 
                 [ring/ring-core "1.1.7"]
                 [org.clojure/data.csv "0.1.2"] ;; For load  purposes
                 [com.cemerick/url "0.1.0"]
                 [liberator "0.9.0"]
                 [instaparse "1.2.2"]
                 [http-kit "2.1.12"]
                 [compojure "1.1.5"]
                 [serializable-fn "1.1.3"]
                 [clojurewerkz/vclock "1.0.0"]
                 [clj-time "0.6.0"]
                 [org.fusesource.leveldbjni/leveldbjni-all "1.7"]
                 [me.raynes/fs "1.4.4"]
                 [http.async.client "0.5.2"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.slf4j/slf4j-log4j12 "1.6.6"]
                 [org.clojure/core.incubator "0.1.3"]
                 [org.apache.lucene/lucene-core "4.4.0"]
                 [org.apache.lucene/lucene-queryparser "4.4.0"]
                 [org.apache.lucene/lucene-analyzers-common "4.4.0"]
                 [org.clojure/data.codec "0.1.0"]
                 [org.apache.lucene/lucene-highlighter "4.4.0"]
                 
                 ;; This is for the admin UI (urgh)
                 [org.clojure/clojurescript "0.0-2080"]
                 [prismatic/dommy "0.1.1"] ]

  :repositories {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}
  :profiles {:dev {
                   :dependencies [[speclj "2.7.2"]
                                  [spyscope "0.1.3"]
                                  [redl "0.2.0"] ]}
                    :injections  [(require 'spyscope.core)
                                  (require '[redl complete core])]
             }
  :plugins [[speclj "2.7.2"]
            [lein-cljsbuild "1.0.1-SNAPSHOT"] ]

  :cljsbuild { 
    :builds [{:id "admin"
              :source-paths ["src-cljs"]
              :compiler {
                :output-to "admin/admin.js"
                :output-dir "admin/out"
                :optimizations :none
                :source-map true}}]} 
  
  :main cravendb.http
  :test-paths ["spec/"])

