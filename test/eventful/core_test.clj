(ns eventful.core-test
  (:require [midje.sweet :refer :all]
            [midje.repl :refer [autotest]]
            [eventful.core :as core :refer :all]
            [eventful.json])
  (:import (eventstore.j EsConnection EsTransaction)
           (java.util UUID)
           (org.joda.time DateTime)
           (eventstore EsException)
           (eventstore.util ActorCloseable)))

(def
  ^{:doc "If projections are running, tests which read all streams will fail."}
  projections-are-running false)

(defmacro nopr [& args] (when (not projections-are-running) `(fact ~@args)))

(def connection-settings
  {:hostname "127.0.0.1" :port 1113 :login "admin" :password "changeit"})

(defn ! [ref] (try (deref ref 10000 :timeout) (catch Exception e (ex-data e))))

(fact "connect returns an EsConnection and disconnect returns :done"
      (let [conn (connect connection-settings)]
        conn => (partial instance? EsConnection)
        (! (disconnect conn)) => :done))

(def opts (atom nil))

(def log (atom nil))

(defn any-exp-ver-opts [] (assoc @opts :exp-ver :any))

(background
  (around :facts
          (do
            (when @opts (println "[WARN] @opts is not nil:" @opts))
            (reset! opts {:conn   (connect connection-settings)
                          :stream (str "eventful-" (UUID/randomUUID))})
            (reset! log [])
            ?form
            (! (delete-stream (any-exp-ver-opts)))
            @(disconnect (:conn @opts))
            (reset! opts nil))))

(def foobar {:foo :bar})

(defn pos?' [{:keys [commit prepare]}] (and (number? commit) (number? prepare)))

(fact "writing & reading events"
      (let [event1 foobar
            event2 [1.2 #inst"2018-01-04T19:28:42.772-00:00" (UUID/randomUUID)]
            w (! (write-events (any-exp-ver-opts) event1 event2))]
        w => (partial map?)
        (:pos w) => pos?'
        (:next-exp-ver w) => 1
        w =not=> #(contains? % :error-type)
        (! (read-event @opts 0)) => event1
        (! (read-event @opts 1)) => event2
        (! (read-event @opts nil)) => event2))

(fact "writing & reading event metadata"
      (let [ev1-meta {:id (UUID/randomUUID) :type "foo-event" :meta #{:foo}}
            event1 (with-meta {:bar :baz} ev1-meta)
            event2 {:foo :baz}
            _ (! (write-events (any-exp-ver-opts) event1 event2))
            actual1 (! (read-event @opts 0))
            actual2 (! (read-event @opts 1))
            meta1 (meta actual1)
            meta2 (meta actual2)]
        actual1 => event1
        actual2 => event2
        (dissoc meta1 :date) => (assoc ev1-meta :num 0 :stream (:stream @opts))
        (:date meta1) => #(.isAfter % (.minusMinutes (DateTime.) 1))
        (:id meta2) => (partial instance? UUID)
        (:type meta2) => "event"
        meta2 =not=> #(contains? % :meta)))

(fact "writing & reading an event as JSON"
      (let [format-opts {:format :json :meta-format :json}
            event (with-meta foobar {:meta foobar})
            _ (! (write-events (merge (any-exp-ver-opts) format-opts) event))
            actual (! (read-event (merge @opts format-opts) 0))
            expected {"foo" "bar"}]
        actual => expected
        (-> actual meta :meta) => expected))

(def bytes-opts {:format :bytes :meta-format :bytes})

(fact "writing & reading events as bytes"
      (let [bytes (.getBytes "foo")
            event1 (with-meta {::core/val bytes} {:meta bytes})
            event2 (.getBytes "bar")
            _ (! (write-events (merge (any-exp-ver-opts) bytes-opts)
                               event1 event2))
            actual1 (! (read-event (merge @opts bytes-opts) 0))
            actual2 (! (read-event (merge @opts bytes-opts) 1))]
        (-> actual1 ::core/val vec) => (vec bytes)
        (-> actual1 meta :meta vec) => (vec bytes)
        (-> actual2 ::core/val vec) => (vec event2)))

(fact "writing & reading naked primitives as events"
      (let [event1 (with-meta {::core/val 100} {:meta 1/3})
            event2 "foo"
            event3 true
            _ (! (write-events (any-exp-ver-opts) event1 event2 event3))
            actual1 (! (read-event @opts 0))
            actual2 (! (read-event @opts 1))
            actual3 (! (read-event @opts 2))]
        (::core/val actual1) => 100
        (-> actual1 meta :meta) => 1/3
        (::core/val actual2) => "foo"
        (-> actual2 meta :id) => (partial instance? UUID)
        (::core/val actual3) => true))

(fact "errors on write"
      (let [w1 (! (write-events (assoc @opts :exp-ver 1000) foobar))
            w2 (! (write-events (assoc @opts :exp-ver :any :login "?"
                                             :password "?") foobar))
            w3 (! (write-events (assoc @opts :exp-ver :no-stream) foobar))
            w4 (! (write-events (assoc @opts :exp-ver :no-stream) foobar))]
        (:error-type w1) => :wrong-exp-ver
        (:error w1) => (partial instance? EsException)
        (:error-type w2) => :not-authenticated
        w3 =not=> #(contains? % :error-type)
        w3 =not=> #(contains? % :error)
        (:error-type w4) => :wrong-exp-ver))

(fact "deleting a stream"
      (let [_ (! (write-events (any-exp-ver-opts) foobar))
            d (! (delete-stream (any-exp-ver-opts)))
            r (! (read-event @opts 0))]
        (:pos d) => pos?'
        (:error-type r) => :stream-not-found))

(fact "transactions"
      (let [tx1 (! (tx-start (any-exp-ver-opts)))
            tx2 (! (tx-cont (select-keys @opts [:conn]) (tx-id tx1)))
            events [(with-meta foobar {:meta {:baz :foo}}) {:baz :bar}]
            w (! (apply tx-write-events {:tx tx2} events))
            r (! (read-event @opts 0))
            c (! (tx-commit tx2))
            actual1 (! (read-event @opts 0))
            actual2 (! (read-event @opts 1))
            _ @(disconnect (:conn @opts))
            tx3 (! (tx-cont (select-keys @opts [:conn]) (tx-id tx1)))]
        [tx1 tx2] => (partial every? (partial instance? EsTransaction))
        w => :done
        (:error-type r) => :stream-not-found
        c => :done
        [actual1 actual2] => events
        (-> actual1 meta :meta :baz) => :foo
        (:error tx3) => (partial instance? Exception)))

(fact "reading a stream"
      (let [events (mapv #(with-meta {:foo %} {:meta {:bar %}}) (range 5))
            idxs (fn [& i] (let [exp (reduce #(conj %1 (events %2)) [] i)]
                             #(and (= % exp) (= (map (comp meta :meta) %)
                                                (map (comp meta :meta) exp)))))
            _ (! (apply write-events (any-exp-ver-opts) events))
            m (-> (read-stream @opts [nil 1000]) ! meta)]
        (! (read-stream @opts [2 2])) => (idxs 2 3)
        (! (read-stream @opts [2 1000])) => (idxs 2 3 4)
        (! (read-stream @opts [nil 1])) => (idxs 0)
        (! (read-stream @opts [nil 4])) => (idxs 0 1 2 3)
        (! (read-stream @opts [4 -3])) => (idxs 4 3 2)
        (! (read-stream @opts [2 -1000])) => (idxs 2 1 0)
        (! (read-stream @opts [nil -1])) => (idxs 4)
        (! (read-stream @opts [nil -4])) => (idxs 4 3 2 1)
        (get-in m [:event-num :next]) => number?
        (get-in m [:event-num :last]) => number?
        (:end-of-stream m) => true?
        (:last-commit-pos m) => number?
        (:dir m) => :forward
        (-> (read-stream @opts [1 -1]) ! meta :dir) => :backward))

(nopr "reading all streams"
      (let [str2 (-> @opts :stream (str "-two"))
            event0 (with-meta [0] {:meta foobar})
            _ (! (write-events (any-exp-ver-opts) event0 [1] [2]))
            _ (! (write-events (assoc (any-exp-ver-opts) :stream str2) [3] [4]))
            r-opts (-> @opts (dissoc :stream))
            actual1 (! (read-all-streams r-opts -3))
            pos (-> actual1 meta :next-pos)
            actual2 (! (read-all-streams (assoc r-opts :pos pos) -2))
            actual3 (! (read-all-streams (assoc r-opts :pos pos) 2))]
        (! (delete-stream (assoc (any-exp-ver-opts) :stream str2)))
        actual1 => [[4] [3] [2]]
        actual2 => [[1] [0]]
        actual3 => [[2] [3]]
        (map #(-> % meta :stream) actual1) => [str2 str2 (:stream @opts)]
        (-> actual1 first meta :pos) => pos?'
        (-> actual2 last meta :meta) => foobar
        pos => pos?'
        (-> actual2 meta :this-pos) => pos
        (-> actual2 meta :dir) => :backward
        (-> actual3 meta :dir) => :forward))

(fact "reading all streams from start"
      (! (read-all-streams (-> @opts (dissoc :stream) (merge bytes-opts)) 20))
      => #(= (count %) 20))

(defn append [x] (swap! log conj x))

(defn callbacks
  ([id & only]
   (-> (callbacks id) (select-keys only)))
  ([id]
   {:live  #(append [id :live])
    :event #(append [id %])
    :error #(append [id :error %])
    :close #(append [id :close])}))

(defn wait-for [c] (! (future (while (< (count @log) c) (Thread/sleep 10)))))

(fact "subscribing to a stream"
      (let [_ (! (write-events (any-exp-ver-opts) [0]
                               (with-meta [1] {:meta {1 1}})))
            sub1 (subscribe-to-stream (assoc @opts :from 0) (callbacks 1))
            sub2 (subscribe-to-stream (assoc @opts :from 3) (callbacks 2))
            sub3 (subscribe-to-stream @opts (callbacks 3 :event))]
        (wait-for 4) => nil
        (! (write-events (any-exp-ver-opts) [2] [3])) => anything
        (wait-for 9) => nil
        [(close-subscription sub1) (close-subscription sub2)] => [nil nil]
        (wait-for 11) => nil
        (! (write-events (any-exp-ver-opts) (with-meta [4] {:meta {4 4}})))
        => anything
        (wait-for 12) => nil
        @log => (just [[1 :live] [2 :live] [1 :close] [2 :close]
                       [1 [0]] [1 [1]] [1 [2]] [1 [3]] [2 [3]] [3 [2]] [3 [3]]
                       [3 [4]]] :in-any-order)
        @log => (contains [[1 [0]] [1 [1]] [1 :live] [1 [2]] [1 [3]] [1 :close]]
                          :gaps-ok)
        @log => (contains [[2 :live] [2 [3]] [2 :close]] :gaps-ok)
        @log => (contains [[3 [2]] [3 [3]] [3 [4]]] :gaps-ok)
        (map (comp :meta meta second) @log) => (contains [{1 1} {4 4}] :gaps-ok)
        (close-subscription sub3)))

(nopr "subscribing to all streams"
      (let [str2 (-> @opts :stream (str "-two"))
            _ (! (write-events (any-exp-ver-opts) [0] [1]))
            _ (! (write-events (assoc (any-exp-ver-opts) :stream str2)
                               (with-meta [2] {:meta {2 2}})))
            pos0 (-> (read-all-streams @opts -3) ! last meta :pos)
            sub1 (subscribe-to-all-streams
                   (-> @opts (dissoc :stream) (assoc :pos pos0)) (callbacks 1))
            _ (assert (nil? (wait-for 3)))
            pos1 (-> @log first second meta :pos)
            sub2 (subscribe-to-all-streams
                   (-> @opts (dissoc :stream) (assoc :pos pos1)) (callbacks 2))
            sub3 (subscribe-to-all-streams (-> @opts (dissoc :stream))
                                           (callbacks 3 :event))
            _ (assert (nil? (wait-for 5)))
            _ (close-subscription sub1)]
        (! (write-events (any-exp-ver-opts) (with-meta [3] {:meta {3 3}})))
        (wait-for 8) => nil
        @log => (just [[1 :live] [2 :live] [1 :close]
                       [1 [1]] [1 [2]] [2 [2]] [2 [3]] [3 [3]]] :in-any-order)
        @log => (contains [[1 [1]] [1 [2]] [1 :live] [1 :close]] :gaps-ok)
        @log => (contains [[2 [2]] [2 :live] [2 [3]]] :gaps-ok)
        @log => (contains [[3 [3]]])
        (map (comp :meta meta second) @log) => (contains [{2 2} {3 3}] :gaps-ok)
        (take 2 (map (comp :stream meta second) @log)) => [(:stream @opts) str2]
        [(close-subscription sub2) (close-subscription sub3)] => [nil nil]
        (! (delete-stream (assoc (any-exp-ver-opts) :stream str2)))))

(fact "subscribing to all streams from start"
      (let [opts' (-> @opts (dissoc :stream) (assoc :pos :zero)
                      (merge bytes-opts))
            sub (subscribe-to-all-streams opts' (callbacks 1))]
        (wait-for 20) => nil
        (close-subscription sub)))

(fact "setting & getting stream metadata"
      (let [s (! (set-stream-metadata (any-exp-ver-opts) foobar))]
        s => (partial map?)
        (:pos s) => pos?'
        (:next-exp-ver s) => 0
        s =not=> #(contains? % :error-type)
        (! (get-stream-metadata @opts)) => foobar))

(fact "setting & getting stream metadata as JSON"
      (! (set-stream-metadata (assoc (any-exp-ver-opts) :format :json) foobar))
      (! (get-stream-metadata (assoc @opts :format :json))) => {"foo" "bar"})

(defn delete-persistent-subscriptions
  [& gs]
  (doseq [g gs] (delete-persistent-subscription (assoc @opts :group g))))

(defn group-opts [group] (assoc @opts :group group))

(fact "persistent subscription"
      (let [_ (! (create-persistent-subscription (group-opts "a") nil))
            _ (! (create-persistent-subscription (group-opts "b") nil))
            sub1 (persistently-subscribe (group-opts "a") (callbacks 1))
            sub2 (persistently-subscribe (group-opts "b") (callbacks 2 :event))]
        sub1 => (partial instance? ActorCloseable)
        (wait-for 1) => nil
        (! (write-events (any-exp-ver-opts) (with-meta [0] {:meta foobar})
                         [1] [2])) => anything
        (wait-for 7) => nil
        (close-subscription sub1) => nil
        (! (write-events (any-exp-ver-opts) [3])) => anything
        (wait-for 8) => nil
        @log => (just [[1 :live] [1 [0]] [1 [1]] [1 [2]]
                       [2 [0]] [2 [1]] [2 [2]] [2 [3]]] :in-any-order)
        @log => (contains [[1 :live] [1 [0]] [1 [1]] [1 [2]]] :gaps-ok)
        (map (comp :meta meta second) @log) => (contains [foobar])
        (close-subscription sub2)
        (delete-persistent-subscriptions "a" "b")))

(fact "persistent subscription from"
      (let [_ (! (create-persistent-subscription (group-opts "a") nil))
            _ (! (write-events (any-exp-ver-opts) [0] [1]))
            _ (! (create-persistent-subscription (group-opts "b") nil))
            _ (! (create-persistent-subscription (group-opts "c") {:from 1}))
            _ (! (write-events (any-exp-ver-opts) [2]))]
        (with-open [_ (persistently-subscribe (group-opts "a") (callbacks 1))
                    _ (persistently-subscribe (group-opts "b") (callbacks 2))
                    _ (persistently-subscribe (group-opts "c") (callbacks 3))]
          (wait-for 9) => nil
          @log => (just [[1 :live] [2 :live] [3 :live] [1 [0]] [1 [1]] [1 [2]]
                         [2 [2]] [3 [1]] [3 [2]]] :in-any-order))
        (delete-persistent-subscriptions "a" "b" "c")))

(fact "strategy of a persistent subscription"
      (! (create-persistent-subscription (group-opts "a")
                                         {:strategy :round-robin}))
      (! (create-persistent-subscription (group-opts "b")
                                         {:strategy :dispatch-to-single}))
      (with-open
        [_ (persistently-subscribe (group-opts "a") (callbacks 1 :event))
         _ (persistently-subscribe (group-opts "a") (callbacks 2 :event))
         _ (persistently-subscribe (group-opts "b") (callbacks 3 :event))
         _ (persistently-subscribe (group-opts "b") (callbacks 4 :event))]
        (let [events (mapv vector (range 5))
              _ (! (apply write-events (any-exp-ver-opts) events))
              xor #(and (or %1 %2) (or (not %1) (not %2)))]
          (wait-for 10) => nil
          (set (map first @log)) => #(and (% 1) (% 2) (xor (% 3) (% 4)))))
      (delete-persistent-subscriptions "a" "b"))

(fact "max subscribers count of a persistent subscription"
      (! (create-persistent-subscription (group-opts "a") {:max-subscribers 1}))
      (with-open [_ (persistently-subscribe (group-opts "a") (callbacks 1))
                  _ (persistently-subscribe (group-opts "a") (callbacks 2))]
        (wait-for 2) => nil
        (->> @log (map #(take 2 %)) (sort-by first))
        => #(#{[[1 :live] [2 :error]] [[1 :error] [2 :live]]} %))
      (delete-persistent-subscriptions "a"))

(fact "timeout and auto/manual ack of a persistent subscriptions"
      (! (create-persistent-subscription (group-opts "a")
                                         {:timeout-ms 10}))
      (! (create-persistent-subscription (group-opts "b")
                                         {:timeout-ms 10}))
      (with-open
        [sub1 (persistently-subscribe (group-opts "a") (callbacks 1 :event))
         sub2 (persistently-subscribe
                (assoc (group-opts "b") :auto-ack false) (callbacks 2 :event))]
        (! (write-events (any-exp-ver-opts) [0] [1]))
        (wait-for 4) => nil
        (let [event (-> (filter #{[2 [1]]} @log) first second)]
          (manual-ack sub2 event)) => nil
        (wait-for 5) => nil
        (count (filter #{[1 [0]]} @log)) => #(= % 1)
        (count (filter #{[2 [0]]} @log)) => #(>= % 2)
        (count (filter #{[2 [1]]} @log)) => #(= % 1))
      (delete-persistent-subscriptions "a" "b"))

(fact "errors on persistent subscription"
      (with-open
        [_ (persistently-subscribe (group-opts "a") (callbacks 1))
         _ (persistently-subscribe (group-opts "b") (callbacks 2 :event))]
        (wait-for 1) => nil
        (->> @log first (take 2)) => [1 :error]
        (-> @log first (nth 2) :error-type) => :other
        (-> @log first (nth 2) :error) => (partial instance? EsException)))

(fact "creating, updating and deleting a persistent subscription"
      (let [opts' (group-opts "a")]
        (! (create-persistent-subscription opts' {:from 0})) => :done
        (! (update-persistent-subscription opts' {:from 1})) => :done
        (! (delete-persistent-subscription opts')) => :done
        (:error (! (update-persistent-subscription opts' {:from 1}))) => truthy
        (:error-type (! (delete-persistent-subscription opts'))) => truthy))

(fact "filtering in subscriptions and when reading streams"
      (! (create-persistent-subscription (group-opts "a") nil))
      (let [f (fn [id] (fn [x] (append [id :where x]) (#{"x" "z"} (:type x))))
            callbacks-w-filter #(assoc (callbacks % :event) :where (f %))]
        (with-open
          [_ (subscribe-to-stream @opts (callbacks-w-filter 1))
           _ (subscribe-to-all-streams @opts (callbacks-w-filter 2))
           _ (persistently-subscribe (group-opts "a") (callbacks-w-filter 3))]
          (! (apply write-events (assoc (any-exp-ver-opts) :meta-format :bytes)
                    (map #(with-meta [%1] {:type %2 :meta (byte-array 1 [%1])})
                         [0 1 2] ["x" "y" "z"])))
          (wait-for 15) => nil
          (->> @log (filter (comp vector? last)) (sort-by first))
          => [[1 [0]] [1 [2]] [2 [0]] [2 [2]] [3 [0]] [3 [2]]]
          (! (read-stream (assoc @opts :where (f 4)) [nil -3])) => [[2] [0]]
          (! (read-all-streams (-> @opts (dissoc :stream) (assoc :where (f 5)))
                               -10)) => [[2] [0]]
          (map #(update % 2 (comp vec :meta)) (filter #(= (count %) 3) @log))
          => (contains #{[1 :where [0]] [2 :where [1]] [3 :where [2]]
                         [4 :where [0]] [5 :where [1]]} :gaps-ok)))
      (delete-persistent-subscriptions "a"))

(fact "getting errors on deref"
      (let [err1 (write-events (assoc @opts :exp-ver 1000) foobar)
            err2 (read-event @opts 0)
            succ (write-events (any-exp-ver-opts) foobar)
            !' #(deref % 10000 :timeout)]
        (!' err1) => (throws clojure.lang.ExceptionInfo
                             #(and (= (-> % ex-data :error-type) :wrong-exp-ver)
                                   (instance? EsException (-> % ex-data :error))
                                   (seq (.getMessage %))))
        (!' err2) => (throws clojure.lang.ExceptionInfo
                             #(= (-> % ex-data :error-type) :stream-not-found))
        (!' succ) => #(number? (:next-exp-ver %))))

(fact "reducing a stream"
      (let [vs (atom [])
            rs read-stream
            events (mapv vector (range 5))
            f #(+ %1 (first %2))]
        (with-redefs [read-stream (fn [m v] (swap! vs conj v) (rs m v))]
          (! (apply write-events (any-exp-ver-opts) events))
          (unwrap (! (reduce-stream (assoc @opts :init 5 :batch-sz 2) f))) => 15
          @vs => [[nil 2] [2 2] [4 2]])))

(fact "metadata of a stream reduction"
      (! (write-events (any-exp-ver-opts) foobar))
      (-> (reduce-stream @opts (constantly ^{:baz true} {})) ! meta)
      => #(and (:baz %) (:last-commit-pos %)))
