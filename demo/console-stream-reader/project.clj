(defproject console-stream-reader "1.0.0"
  :description "Console Stream Reader"
  :url "https://github.com/PawelStroinski/eventful"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.cli "0.3.5"]
                 [eventful "1.0.0"]
                 [mvxcvi/puget "1.0.2"]
                 [cheshire "5.7.0"]
                 [com.cognitect/transit-clj "0.8.309"]]
  :main ^:skip-aot console-stream-reader.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :uberjar-merge-with {#"\.conf$" [slurp str spit]})
