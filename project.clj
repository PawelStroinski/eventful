(defproject eventful "0.1.0-alpha5"
  :description "Event Store client library"
  :url "https://github.com/PawelStroinski/eventful"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.geteventstore/eventstore-client_2.12 "5.0.8"]
                 [manifold "0.1.8"]
                 [com.cognitect/transit-clj "0.8.309"]]
  :plugins [[lein-codox "0.10.3"]]
  :codox
  {:namespaces [eventful.core]
   :source-uri
   "https://github.com/PawelStroinski/eventful/blob/{version}/{filepath}#L{line}"}
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.9.0"]
                                  [midje "1.9.1"]
                                  [org.clojure/test.check "0.9.0"]
                                  [cheshire "5.8.0"]]}})
