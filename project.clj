(defproject cravendb "0.0.0"
  :min-lein-version "2.2.0"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clj-http "0.7.6"]
                 [ring "1.2.0"]
                 [compojure "1.1.5"]
                 [serializable-fn "1.1.3"]
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
                 [org.apache.lucene/lucene-highlighter "4.4.0"]]


  :repositories {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}
            
  :profiles {:dev {
                   :dependencies [[speclj "2.7.0"]
                                  [speclj-growl "2.1.0"]]}}
  :plugins [[speclj "2.7.0"]]
  :main cravendb.http
  :test-paths ["spec/"])

