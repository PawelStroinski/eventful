(defproject eventful "0.1.0-alpha2"
  :description "Event Store client library"
  :url "https://github.com/PawelStroinski/eventful"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.geteventstore/eventstore-client_2.12 "5.0.1"]
                 [manifold "0.1.6"]]
  :plugins [[lein-codox "0.10.3"]]
  :codox
  {:namespaces [eventful.core]
   :source-uri
   "https://github.com/PawelStroinski/eventful/blob/{version}/{filepath}#L{line}"}
  :profiles {:dev {:dependencies [[midje "1.9.1"]
                                  [org.clojure/test.check "0.9.0"]
                                  [cheshire "5.7.0"]]}})
