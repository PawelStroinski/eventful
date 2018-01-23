(ns eventful.stress-test
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [eventful.core :refer :all]
            [eventful.core-test :as test :refer [! log]]))

(def ns-str (str *ns*))

(defonce conn (connect (assoc test/connection-settings :conn-name ns-str)))

(defn read-written-event
  [stream event & {:keys [exp-ver next-exp-ver?]}]
  (let [write-result (! (write-events {:conn    conn :stream stream
                                       :exp-ver exp-ver} event))
        read-result (! (read-event {:conn conn :stream stream} nil))]
    (or (and (next-exp-ver? (:next-exp-ver write-result))
             (= read-result event))
        (prn "***" stream "*" write-result "*" read-result "*" event))))

(def read-written-event-any-exp-ver-prop
  (prop/for-all
    [stream (gen/such-that #(not (.startsWith % "$"))
                           (gen/not-empty gen/string-ascii))
     event (gen/map gen/keyword gen/string-ascii)]
    (read-written-event stream event :exp-ver :any :next-exp-ver? number?)))

(defonce stream-count (atom 0))

(defn next-stream [] (str ns-str (swap! stream-count inc)))

(def read-written-event-strict-exp-ver-prop
  (prop/for-all
    [event (gen/map gen/keyword gen/string-ascii)]
    (read-written-event (next-stream) event :exp-ver :no-stream
                        :next-exp-ver? (every-pred some? zero?))))

(defn write-events-in-tx-while-in-persistent-sub-prop
  []
  (let [stream (next-stream)
        opts {:conn conn :stream stream :group "foo"}]
    (! (create-persistent-subscription opts nil))
    (reset! log [])
    [(persistently-subscribe opts (test/callbacks 1 :event :error))
     (prop/for-all
       [[event1 event2] (gen/vector (gen/map gen/keyword gen/string-ascii) 2)]
       (let [c (count @log)
             tx (! (tx-start {:conn conn :stream stream :exp-ver (dec c)}))
             w1 (tx-write-events {:tx tx} event1)
             w2 (tx-write-events {:tx tx} event2)
             cr (tx-commit tx)
             w (test/wait-for (+ c 2))]
         (or (= (map last (subvec @log c)) [event1 event2])
             (prn "***" stream "*" c "*" (! w1) "*" (! w2) "*" (! cr) "*" w "*"
                  event1 "*" event2 "*" (subvec @log c)))))]))

(defn write-json-events-while-sub-to-all-streams-prop
  []
  (reset! log [])
  [(subscribe-to-all-streams {:conn conn :format :json}
                             (assoc (test/callbacks 2 :event :error)
                               :where #(.startsWith (:stream %) ns-str)))
   (prop/for-all
     [[event1 event2] (gen/vector (gen/map gen/string-ascii gen/int) 2)]
     (let [c (count @log)
           stream (next-stream)
           wr (write-events {:conn   conn :stream stream :exp-ver :no-stream
                             :format :json} event1 event2)
           w (test/wait-for (+ c 2))]
       (or (= (map last (subvec @log c)) [event1 event2])
           (prn "***" c "*" stream "*" (! wr) "*" w "*" event1 "*" event2 "*"
                (subvec @log c)))))])

(defn quick-check-sub [n [sub p]] (with-open [_ sub] (tc/quick-check n p)))

(comment                                ; Be patient
  (tc/quick-check 1000 read-written-event-any-exp-ver-prop)
  (tc/quick-check 1000 read-written-event-strict-exp-ver-prop)
  (quick-check-sub 185 (write-events-in-tx-while-in-persistent-sub-prop))
  (quick-check-sub 100000 (write-json-events-while-sub-to-all-streams-prop)))
