(defproject eventful "1.0.0"
  :description "EventStoreDB client library"
  :url "https://github.com/PawelStroinski/eventful"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.geteventstore/eventstore-client_2.12 "7.0.2"]
                 [manifold "0.1.6"]
                 ;; joda-time added because eventstore-client moved to java.time
                 [joda-time "2.10"]]
  :plugins [[lein-codox "0.10.7"]]
  :codox
  {:namespaces [eventful.core]
   :source-uri
   "https://github.com/PawelStroinski/eventful/blob/{version}/{filepath}#L{line}"}
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.1"]
                                  [midje "1.9.9"]
                                  [org.clojure/test.check "0.9.0"]
                                  [cheshire "5.8.0"]
                                  [com.cognitect/transit-clj "0.8.309"]]}})
