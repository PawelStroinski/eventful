(ns console-stream-reader.core
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [eventful.core :as evf]
            [eventful.json]
            [eventful.transit]
            [puget.printer :refer [cprint]]))

(defn str->int [x] (Integer/parseInt x))

(def option-specs
  [["-H" "--hostname X" "ES address" :default "127.0.0.1"]
   ["-P" "--port X" "ES address" :default 1113 :parse-fn str->int]
   ["-L" "--login X" "ES user" :default "admin"]
   ["-p" "--password X" "ES user" :default "changeit"]
   ["-s" "--stream X" "Stream name (required)"]
   ["-c" "--max-count X" "Count (reads tail if negative)" :parse-fn str->int]
   ["-S" "--start X" "Start from event number" :parse-fn str->int]
   ["-l" "--live" "Live subscription"]
   ["-m" "--meta" "Include event metadata"]
   ["-j" "--json" "Deserialize plain JSON instead of EDN"]
   ["-t" "--transit" "Deserialize Transit-JSON instead of EDN"]
   ["-h" "--help" "Print this help"]])

(defn valid? [{:keys [stream max-count live]}] (and stream (or max-count live)))

(defn with-conn
  [m f]
  (let [conn (evf/connect (select-keys m [:hostname :port :login :password]))]
    (try (f (assoc m :conn conn))
         (finally @(evf/disconnect conn) (shutdown-agents)))))

(defn opts
  [m]
  (cond-> (select-keys m [:conn :stream])
          (:transit m) (assoc :format :transit :meta-format :transit)
          (:json m) (assoc :format :json :meta-format :json)))

(defn print-event [x m] (cprint x {:print-meta (:meta m)}) (println))

(defn print-stream
  [{:keys [start max-count] :as m}]
  (let [xs @(evf/read-stream (opts m) [start max-count])]
    (doseq [x (if (pos? max-count) xs (reverse xs))]
      (print-event x m))))

(defn subscribe
  [m]
  (with-open [_ (evf/subscribe-to-stream (opts m) {:event #(print-event % m)})]
    (println "Press Enter to close subscription...\n")
    (read-line)))

(defn main
  [{:keys [max-count live] :as m}]
  (when max-count (print-stream m))
  (when live (subscribe m)))

(defn -main
  [& args]
  (let [{:keys [options summary]} (cli/parse-opts args option-specs)]
    (if (or (:help options) (not (valid? options)))
      (println (str "Console Stream Reader\n\n" summary))
      (with-conn options main))))
