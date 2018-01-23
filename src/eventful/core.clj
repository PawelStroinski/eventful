(ns eventful.core
  "Event Store (https://eventstore.org/) client library.
  Wrapper around the JVM Client (TCP interface).

  Provides (hopefully) Clojure-friendly functions to work with the Event Store.
  Supports EDN & JSON formats as well as raw bytes. It's easy to add more.

  This namespace covers (almost) all of the API of the JVM Client and this
  documentation often copies its documentation when it may be helpful."
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import (eventstore.j SettingsBuilder EventDataBuilder EsConnectionFactory
                         EsConnection EsTransaction WriteEventsBuilder
                         PersistentSubscriptionSettingsBuilder)
           (java.net InetSocketAddress)
           (akka.actor ActorSystem Props UntypedActor Status$Failure FSM$Event
                       Actor RepointableActorRef)
           (clojure.lang Agent IObj)
           (java.util UUID)
           (java.io ByteArrayOutputStream PushbackReader Closeable)
           (eventstore
             Content ExpectedVersion ExpectedVersion$Any$
             ExpectedVersion$NoStream$ WriteResult WrongExpectedVersionException
             EventNumber Event DeleteResult Position$Exact UserCredentials
             StreamNotFoundException NotAuthenticatedException IndexedEvent
             ReadStreamEventsCompleted ReadDirection$Forward$ EventNumber$Exact
             ReadAllEventsCompleted Position PersistentSubscriptionActor
             SubscriptionObserver EventStream EventRecord LiveProcessingStarted$
             PersistentSubscriptionActor$ManualAck WithCredentials
             WriteEventsCompleted EventNumber$Range)
           (akka.dispatch OnSuccess OnFailure)
           (scala.concurrent ExecutionContext Future)
           (akka.japi Creator)
           (scala Option)
           (java.util.concurrent TimeUnit)
           (eventstore.cluster ClusterSettings)
           (eventstore.tcp ConnectionActor)
           (eventstore.util ActorCloseable)))

(defn- num?? [x] (or (not x) (number? x)))

(defn- bool? [x] (or (true? x) (false? x)))

(defn- bool?? [x] (or (nil? x) (bool? x)))

(defn- overflow-strat??
  [x]
  (or (not x) (#{:drop-head :drop-tail :drop-buf :drop-new :fail} x)))

(defn- cluster-settings?? [x] (or (not x) (instance? ClusterSettings x)))

(def ^:private ms TimeUnit/MILLISECONDS)

(defn connect
  "Returns a full duplex connection to the event store.

  All operations are handled in a full async manner. Many threads can use this
  connection at the same time or a single thread can make many asynchronous
  requests. To get the most performance out of the connection it is generally
  recommended to use it in this way.

  Options:
  :hostname          - (required) the event store address
  :port              - ”
  :login             - (required) user credentials to perform operation with
  :password          - ”
  :conn-timeout-ms   - the desired connection timeout (milliseconds, 1000 by
                       default)
  :max-reconn        - maximum number of reconnections before backing off, -1
                       to reconnect forever (100 by default)
  :reconn-del-min-ms - delay before first reconnection (milliseconds, 250 by
                       default)
  :reconn-del-max-ms - maximum delay on reconnections (milliseconds, 10000 by
                       default)
  :heartb-int-ms     - the interval at which to send heartbeat messages
                       (milliseconds, 500 by default)
  :heartb-timeout-ms - the interval after which an unacknowledged heartbeat will
                       cause the connection to be considered faulted and
                       disconnect (milliseconds, 5000 by default)
  :max-retries       - the maximum number of operation retries (10 by default)
  :timeout-ms        - the amount of time before an operation is considered to
                       have timed out (milliseconds, 30000 by default)
  :read-batch-sz     - number of events to be retrieved by client as single
                       message (500 by default)
  :buf-sz            - the size of the buffer in element count (100000 by
                       default)
  :overflow-strat    - strategy that is used when elements cannot fit inside the
                       buffer (:drop-head, :drop-tail, :drop-buf, :drop-new or
                       :fail which is the default)
  :cluster-settings  - contains settings relating to a connection to a cluster
                       (can be created with eventstore.j.ClusterSettingsBuilder)
  :serial-parallel   - the number of serialization/deserialization functions to
                       be run in parallel (8 by default)
  :serial-ordered    - serialization done asynchronously and these futures may
                       complete in any order, but results will be used with
                       preserved order if set to true (the default)
  :conn-name         - client identifier used to show a friendly name of client
                       in the event store (\"jvm-client\" by default)"
  [{:keys [hostname port login password conn-timeout-ms max-reconn
           reconn-del-min-ms reconn-del-max-ms heartb-int-ms heartb-timeout-ms
           max-retries timeout-ms read-batch-sz buf-sz overflow-strat
           cluster-settings serial-parallel serial-ordered conn-name]}]
  {:pre [(string? hostname) (integer? port) (string? login) (string? password)
         (num?? conn-timeout-ms) (num?? max-reconn) (num?? reconn-del-min-ms)
         (num?? reconn-del-max-ms) (num?? heartb-int-ms)
         (num?? heartb-timeout-ms) (num?? max-retries) (num?? timeout-ms)
         (num?? read-batch-sz) (num?? buf-sz) (overflow-strat?? overflow-strat)
         (cluster-settings?? cluster-settings) (num?? serial-parallel)
         (bool?? serial-ordered) (or (not conn-name) (string? conn-name))]}
  (EsConnectionFactory/create
    (ActorSystem/create "Eventful")
    (-> (SettingsBuilder.)
        (.address (InetSocketAddress. ^String hostname ^int port))
        (.defaultCredentials login password)
        (cond-> conn-timeout-ms (.connectionTimeout conn-timeout-ms ms))
        (cond-> max-reconn (.maxReconnections max-reconn))
        (cond-> reconn-del-min-ms (.reconnectionDelayMin reconn-del-min-ms ms))
        (cond-> reconn-del-max-ms (.reconnectionDelayMax reconn-del-max-ms ms))
        (cond-> heartb-int-ms (.heartbeatInterval heartb-int-ms ms))
        (cond-> heartb-timeout-ms (.heartbeatTimeout heartb-timeout-ms ms))
        (cond-> max-retries (.operationMaxRetries max-retries))
        (cond-> timeout-ms (.operationTimeout timeout-ms ms))
        (cond-> read-batch-sz (.readBatchSize read-batch-sz))
        (cond-> buf-sz (.bufferSize buf-sz))
        (cond-> (= overflow-strat :drop-head) (.dropHeadWhenOverflow))
        (cond-> (= overflow-strat :drop-tail) (.dropTailWhenOverflow))
        (cond-> (= overflow-strat :drop-buf) (.dropBufferWhenOverflow))
        (cond-> (= overflow-strat :drop-new) (.dropNewWhenOverflow))
        (cond-> (= overflow-strat :fail) (.failWhenOverflow))
        (cond-> cluster-settings (.cluster cluster-settings))
        (cond-> serial-parallel (.serializationParallelism serial-parallel))
        (cond-> (some? serial-ordered) (.serializationOrdered serial-ordered))
        (cond-> conn-name (.connectionName conn-name))
        (.build))))

(defn- ^{:source "sunng87/reflection.clj"} private-field
  [^Object obj fn-name-string]
  (let [m (.. obj getClass (getDeclaredField fn-name-string))]
    (.setAccessible m true)
    (.get m obj)))

(defn- ^RepointableActorRef conn-actor
  [conn]
  (-> conn (private-field "connection") (private-field "connection")))

(defmacro ^{:private true :source "org.clojure.gaverhae/okku"} actor
  "Macro used to define an actor. Actually returns a Props object that can be
  passed to the .actorOf method of an ActorSystem, or similarly that can be used
  as the first argument to spawn."
  [& forms]
  `(Props/create ~UntypedActor (proxy [Creator] []
                                 (~'create []
                                   (proxy [UntypedActor] []
                                     ~@forms)))))

(defonce ^:private dispatcher
         (ExecutionContext/fromExecutorService Agent/soloExecutor))

(def ^:private error-types
  {WrongExpectedVersionException :wrong-exp-ver
   StreamNotFoundException       :stream-not-found
   NotAuthenticatedException     :not-authenticated})

(defn- error->map [x] {:error-type (error-types (class x) :other) :error x})

(defn- ->promise
  ([^Future future]
   (->promise future (constantly :done)))
  ([^Future future success-fn]
   (let [p (promise)]
     (.onSuccess future (proxy [OnSuccess] []
                          (onSuccess [x] (deliver p (success-fn x))))
                 dispatcher)
     (.onFailure future (proxy [OnFailure] []
                          (onFailure [x] (deliver p (error->map x))))
                 dispatcher)
     p)))

(defn- listener
  [p success-class success-fn]
  (actor (onReceive
           [x]
           (condp instance? x
             success-class (deliver p (success-fn x))
             Status$Failure (let [c (.cause ^Status$Failure x)]
                              (deliver p (error->map c)))
             (.unhandled ^Actor this x)))))

(defn- conn? [x] (instance? EsConnection x))

(defn disconnect
  "Disconnects connection to the event store. Returns a promise which derefs to
  :done when the system is terminated.

  WARNING: uses a hack to get a private field from the JVM Client!"
  [conn]
  {:pre [(conn? conn)]}
  (-> (.. (conn-actor conn) system terminate) (->promise)))

(defmulti ^Content serialize
          "Serializes x to a format where format is a keyword.

Built-in formats:
EDN    - this is the default format
:json  - cheshire dependency should be added to your project and eventful.json
         namespace should be required first
:bytes - byte arrays

To add a custom format you can use eventful.json namespace as a starting point."
          {:arglists '([x format])}
          (fn [x format] format))

(defmulti deserialize
          "Deserializes bytes to a format where bytes is a byte array and format
is a keyword. Please refer to serialize multimethod for an info about formats."
          {:arglists '([bytes format])}
          (fn [bytes format] format))

(defmethod serialize :default
  [x format]
  (let [s (ByteArrayOutputStream.)]
    (with-open [w (io/writer s)]
      (binding [*out* w]
        (pr x)))
    (Content/apply (.toByteArray s))))

(defmethod serialize :bytes
  [x format]
  (Content/apply ^bytes x))

(defmethod deserialize :default
  [bytes format]
  (with-open [r (io/reader bytes)]
    (edn/read (PushbackReader. r))))

(defmethod deserialize :bytes
  [bytes format]
  bytes)

(defn- exp-ver? [x] (or (number? x) (#{:any :no-stream} x)))

(defn- ^ExpectedVersion exp-ver->obj
  [exp-ver]
  (case exp-ver
    :any (ExpectedVersion$Any$.)
    :no-stream (ExpectedVersion$NoStream$.)
    (ExpectedVersion/apply exp-ver)))

(defn wrap-event
  "If an event is not a clojure.lang.IObj, wraps it in order to be able to add
  Clojure metadata to it. See also write-events fn."
  [event]
  (if (instance? IObj event) event {::event event}))

(defn unwrap-event
  "Reverse of wrap-event fn. See also read-event fn."
  [event]
  (or (::event event) event))

(defn- event-data
  [event {:keys [format meta-format]}]
  (let [{:keys [type id meta]} (meta event)]
    (-> (EventDataBuilder. (or type "event"))
        (.eventId (or id (UUID/randomUUID)))
        (.data (serialize (unwrap-event event) format))
        (cond-> meta (.metadata (serialize meta meta-format)))
        (.build))))

(defn- creds
  [{:keys [login password]}]
  (when (and login password)
    (UserCredentials. login password)))

(defn- with-creds [x m] (if-let [c (creds m)] (WithCredentials. x c) x))

(defn- write-events-msg
  [{:keys [stream exp-ver req-master] :or {req-master true} :as m} events]
  (-> (WriteEventsBuilder. stream)
      (.expectVersion (exp-ver->obj exp-ver))
      (.requireMaster ^boolean req-master)
      (.addEvents ^Iterable (map #(event-data % m) events))
      (.build)
      (with-creds m)))

(defn- write-events-completed->map
  [^WriteEventsCompleted x]
  {:pre [x (.. x numbersRange isDefined)]}
  (let [opt-pos (.position x)
        ^Position$Exact pos (when opt-pos (.getOrElse opt-pos nil))
        ^EventNumber$Range nr (.. x numbersRange get)]
    (conj {:next-exp-ver (.. nr end value)}
          (when pos [:pos {:commit  (.commitPosition pos)
                           :prepare (.preparePosition pos)}]))))

(defn- spawn
  [conn props]
  (.. (conn-actor conn) system (actorOf props)))

(defn- stream? [x] (and (string? x) (seq x)))

(defn write-events
  "Writes events to a stream.

  Options:
  :conn        - (required) connection returned by the connect fn
  :stream      - (required) stream name, e.g. \"inventory-item-1\"
  :exp-ver     - (required) expected version, which is either a number,
                 :any or :no-stream - see notes section below
  :req-master  - should the event store refuse operation if it is not master?
                 (true by default)
  :format      - event serialization format - please refer to serialize
                 multimethod for an info about formats (EDN by default)
  :meta-format - event metadata serialization format (see :format above)
  :login       - optional user credentials to perform operation with
  :password    - ”

  About expected version:
  When writing events to a stream the :exp-ver choice can make a very large
  difference in the observed behavior. For example, if no stream exists and :any
  is used, a new stream will be implicitly created when appending. There are
  also differences in idempotency between different types of calls. If you
  specify an expected version aside from :any the event store will give you an
  idempotency guarantee. If using :any the event store will do its best to
  provide idempotency but does not guarantee idempotency.

  An event can be anything as long as it can be (de)serialized. Please refer to
  serialize and deserialize multimethods for an info about this. It can have
  Clojure metadata with optional values for:
  :type - the type of an event, \"event\" by default
  :id   - the id of an event (UUID), random UUID by default
  :meta - the arbitrary event metadata stored in the event store - anything
          (de)serializable.
  If an event is not a clojure.lang.IObj and you want to add Clojure metadata to
  it, first please wrap it like this: {:eventful.core/event <your event>}. You
  can use the wrap-event convenience fn.

  The return value is a promise which derefs to a map with values for:
  -   in case of a success:
  :next-exp-ver - the next expected version for the stream
  :pos          - the position of the write in the log (has :commit and :prepare
                  subkeys, can be nil)
  -   in case of a failure:
  :error-type - :wrong-exp-ver, :stream-not-found, :not-authenticated or :other
  :error      - a Throwable

  Example:
  (let [conn (connect {:hostname \"127.0.0.1\" :port 1113
                       :login    \"admin\"     :password \"changeit\"})]
    (write-events {:conn conn :stream \"inventory-item-1\" :exp-ver :no-stream}
                  {:event :created :name \"foo\"}))"
  [{:keys [^EsConnection conn stream exp-ver] :as m} & events]
  {:pre [(conn? conn) (stream? stream) (exp-ver? exp-ver) (seq events)]}
  (let [p (promise)
        msg (write-events-msg m events)
        listener (spawn conn (listener p WriteEventsCompleted
                                       write-events-completed->map))]
    (.tell (conn-actor conn) msg listener)
    p))

(defn- event-record-meta
  [^EventRecord event]
  (let [data (.data event)
        meta (.. data metadata value toArray)]
    (conj {:id     (.eventId data)
           :type   (.eventType data)
           :num    (.. event number value)
           :stream (.. event streamId value)
           :date   (.. event created (getOrElse nil))}
          (when (seq meta) [:meta meta]))))

(defn- event-record->map
  [^EventRecord event {:keys [format meta-format]}]
  {:pre [event]}
  (let [raw (.. event data data value toArray)
        meta (event-record-meta event)]
    (with-meta (wrap-event (deserialize raw format))
               (if (contains? meta :meta)
                 (update meta :meta #(deserialize % meta-format))
                 meta))))

(defn- event-meta
  [^Event event]
  {:pre [event]}
  (event-record-meta (.record event)))

(defn- event->map
  [^Event event m]
  {:pre [event]}
  (event-record->map (.record event) m))

(defn read-event
  "Reads a single event from a stream at event number num. If num is nil,
  reads the latest event.

  Options:
  :conn             - (required) see write-events fn
  :stream           - ”
  :resolve-link-tos - whether to resolve LinkTo events automatically (false by
                      default)
  :req-master       - see write-events fn
  :format           - ”
  :meta-format      - ”
  :login            - ”
  :password         - ”

  The successful return value is a promise which derefs to a deserialized event
  which has Clojure metadata with values for:
  :id     - the id of an event (UUID)
  :type   - the type of an event (string)
  :num    - the event number
  :stream - the stream name
  :date   - an org.joda.time.DateTime when the event was added (can be nil)
  :meta   - (optional) the deserialized event metadata
  If an event is not a clojure.lang.IObj, it will be wrapped like this:
  {:eventful.core/event <your event>} in order to be able to add Clojure
  metadata to it. You can use the unwrap-event convenience fn.
  The failed return value - see write-events fn.

  Example:
  (let [conn (connect {:hostname \"127.0.0.1\" :port 1113
                       :login    \"admin\"     :password \"changeit\"})]
    (read-event {:conn conn :stream \"inventory-item-1\"} 0))"
  [{:keys [^EsConnection conn stream resolve-link-tos req-master]
    :or   {resolve-link-tos false req-master true} :as m} num]
  {:pre [(conn? conn) (stream? stream) (bool? resolve-link-tos)
         (bool? req-master) (num?? num)]}
  (-> (.readEvent conn stream (when num (EventNumber/apply ^long num))
                  resolve-link-tos (creds m) req-master)
      (->promise #(event->map % m))))

(defn- pos->map
  [pos]
  (when (instance? Position$Exact pos)
    {:commit  (.commitPosition ^Position$Exact pos)
     :prepare (.preparePosition ^Position$Exact pos)}))

(defn- map->pos
  [{:keys [^long commit ^long prepare]}]
  (when (and commit prepare)
    (Position/apply commit prepare)))

(defn delete-stream
  "Deletes a stream from the event store.

  Options:
  :conn        - (required) see write-events fn
  :stream      - ”
  :exp-ver     - ”
  :hard-delete - indicator for tombstoning vs soft-deleting the stream.
                 tombstoned streams can never be recreated. soft-deleted streams
                 can be written to again, but the event number sequence will not
                 start from 0. (false by default)
  :req-master  - see write-events fn
  :login       - ”
  :password    - ”

  The successful return value is a promise which derefs to a map with a key:
  :pos - see write-events fn
  The failed return value - see write-events fn."
  [{:keys [^EsConnection conn stream exp-ver hard-delete req-master]
    :or   {hard-delete false req-master true} :as m}]
  {:pre [(conn? conn) (stream? stream) (exp-ver? exp-ver) (bool? hard-delete)
         (bool? req-master)]}
  (-> (.deleteStream conn stream (exp-ver->obj exp-ver) hard-delete (creds m)
                     req-master)
      (->promise (fn [^DeleteResult dr]
                   {:pos (when dr (pos->map (.logPosition dr)))}))))

(defn tx-start
  "Starts a transaction in the event store on a given stream asynchronously. A
  transaction allows the calling of multiple writes with multiple round trips
  over long periods of time between the caller and the event store.

  Options:
  :conn       - (required) see write-events fn
  :stream     - (required) the stream to start a transaction on, e.g.
                \"inventory-item-1\"
  :exp-ver    - (required) the expected version of the stream at the time of
                starting the transaction, see write-events fn
  :req-master - see write-events fn
  :login      - ”
  :password   - ”

  The successful return value is a promise which derefs to a transaction object.
  The failed return value - see write-events fn."
  [{:keys [^EsConnection conn stream exp-ver req-master]
    :or   {req-master true} :as m}]
  {:pre [(conn? conn) (stream? stream) (exp-ver? exp-ver) (bool? req-master)]}
  (-> (.startTransaction conn stream (exp-ver->obj exp-ver) (creds m)
                         req-master)
      (->promise identity)))

(defn tx-id
  "Gets id of a transaction tx returned by the tx-start or tx-cont fns."
  [^EsTransaction tx]
  {:pre [(instance? EsTransaction tx)]}
  (.getId tx))

(defn tx-cont
  "Continues transaction by provided transaction id which can be obtained with
  the tx-id fn first.

  Options:
  :conn     - (required) see write-events fn
  :login    - see write-events fn
  :password - ”

  The successful return value is a future which derefs to a new transaction
  object. The failed return value - see write-events fn."
  [{:keys [^EsConnection conn] :as m} ^long id]
  {:pre [(conn? conn) (integer? id)]}
  (future (try (.continueTransaction conn id (creds m))
               (catch Exception e (error->map e)))))

(defn tx-write-events
  "Writes to a transaction in the event store asynchronously.

  Options:
  :tx          - the transaction returned by the tx-start or tx-cont fns
  :format      - see write-events fn
  :meta-format - ”

  Please refer to write-events fn for an info about events to write.

  Returns a promise which derefs to :done on success. The failed return value -
  see write-events fn."
  [{:keys [^EsTransaction tx] :as m} & events]
  {:pre [(instance? EsTransaction tx) (seq events)]}
  (-> (.write tx (map #(event-data % m) events)) (->promise)))

(defn tx-commit
  "Commits this transaction.

  Returns a promise which derefs to :done on success. The failed return value -
  see write-events fn."
  [^EsTransaction tx]
  {:pre [(instance? EsTransaction tx)]}
  (-> (.commit tx) (->promise)))

(defn- fn?? [x] (or (not x) (fn? x)))

(defn- from-num
  [[start max-count]]
  (cond start (EventNumber/apply ^long start)
        (pos? max-count) (EventNumber/First)
        :else (EventNumber/Current)))

(defn- event-num->num
  [^EventNumber x]
  (when (instance? EventNumber$Exact x)
    (.value ^EventNumber$Exact x)))

(defn- dir->kw [x] (if (instance? ReadDirection$Forward$ x) :forward :backward))

(defn- filter-events
  [events & {:keys [where event-fn where-fn]}]
  (if where
    (into [] (comp (filter (comp where where-fn)) (map event-fn)) events)
    (mapv event-fn events)))

(defn- stream->vec
  [^ReadStreamEventsCompleted rs {:keys [where] :as m}]
  {:pre [rs]}
  (with-meta (filter-events (.eventsJava rs) :where where
                            :event-fn #(event->map % m) :where-fn event-meta)
             {:event-num       {:next (event-num->num (.nextEventNumber rs))
                                :last (.. rs lastEventNumber value)}
              :end-of-stream   (.endOfStream rs)
              :last-commit-pos (.lastCommitPosition rs)
              :dir             (dir->kw (.direction rs))}))

(defn- indexed-event-meta
  [^IndexedEvent event]
  {:pre [event]}
  (event-meta (.event event)))

(defn- indexed-event->map
  [^IndexedEvent event m]
  {:pre [event]}
  (-> (event->map (.event event) m)
      (vary-meta assoc :pos (pos->map (.position event)))))

(defn- all-streams->vec
  [^ReadAllEventsCompleted ra {:keys [where] :as m}]
  {:pre [ra]}
  (with-meta (filter-events (.eventsJava ra) :where where
                            :event-fn #(indexed-event->map % m)
                            :where-fn indexed-event-meta)
             {:dir      (dir->kw (.direction ra))
              :this-pos (pos->map (.position ra))
              :next-pos (pos->map (.nextPosition ra))}))

(defn read-stream
  "Reads count events from a stream forwards (e.g. oldest to newest) or
  backwards (e.g. newest to oldest) starting from event number.

  Options:
  :conn             - (required) see write-events fn
  :stream           - ”
  :resolve-link-tos - see read-event fn
  :req-master       - see write-events fn
  :format           - ”
  :meta-format      - ”
  :login            - ”
  :password         - ”
  :where            - see subscribe-to-stream fn

  The second argument determines starting point, direction and count.
  If start is nil, reading starts at the first or at the latest event
  (depeneding on direction). If max-count is positive, direction is forward,
  otherwise is backward. Please refer to eventful.core-test namespace for
  examples.

  The successful return value is a promise which derefs to a vector of events.
  Please refer to read-event fn for an info about returned events. Additionally,
  the vector itself has Clojure metadata with values for:
  :event-num       - a map with the :next event number (can be nil) and the
                     :last event number
  :end-of-stream   - a boolean
  :last-commit-pos - a number
  :dir             - either :forward or :backward
  The failed return value - see write-events fn."
  [{:keys [^EsConnection conn stream resolve-link-tos req-master where]
    :or   {resolve-link-tos false req-master true} :as m}
   [start max-count :as v]]
  {:pre [(conn? conn) (stream? stream) (bool? resolve-link-tos)
         (bool? req-master) (fn?? where) (num?? start) (number? max-count)]}
  (-> (if (pos? max-count)
        (.readStreamEventsForward conn stream (from-num v) max-count
                                  resolve-link-tos (creds m) req-master)
        (.readStreamEventsBackward conn stream (from-num v) (- max-count)
                                   resolve-link-tos (creds m) req-master))
      (->promise #(stream->vec % m))))

(defn read-all-streams
  "Reads all events in the node forward (e.g. beginning to end) or backwards
  (e.g. end to beginning) starting from position.

  Options:
  :conn             - (required) see write-events fn
  :pos              - position to start reading from. if omitted, reading starts
                      at the first or at the latest event (depeneding on
                      direction). for paging use :next-pos returned in metadata.
  :resolve-link-tos - see read-event fn
  :req-master       - see write-events fn
  :format           - ”
  :meta-format      - ”
  :login            - ”
  :password         - ”
  :where            - see subscribe-to-stream fn

  If max-count is positive, direction is forward, otherwise is backward.

  The successful return value is a promise which derefs to a vector of events.
  Please refer to read-event fn for an info about returned events. One extra
  metadata returned with each event is:
  :pos - the event position (has :commit and :prepare subkeys, can be nil)
  Additionally, the vector itself has Clojure metadata with values for:
  :dir      - either :forward or :backward
  :this-pos - the position of this read (has :commit and :prepare subkeys, can
              be nil)
  :next-pos - the next position to use for paging (has :commit and :prepare
              subkeys, can be nil)
  The failed return value - see write-events fn."
  [{:keys [^EsConnection conn pos resolve-link-tos req-master where]
    :or   {resolve-link-tos false req-master true} :as m} max-count]
  {:pre [(conn? conn) (or (not pos) (map? pos)) (bool? resolve-link-tos)
         (bool? req-master) (fn?? where) (number? max-count)]}
  (-> (if (pos? max-count)
        (.readAllEventsForward conn (map->pos pos) max-count
                               resolve-link-tos (creds m) req-master)
        (.readAllEventsBackward conn (map->pos pos) (- max-count)
                                resolve-link-tos (creds m) req-master))
      (->promise #(all-streams->vec % m))))

(defn- dispatch-fn
  [{:keys [event where]} & {:keys [event-fn where-fn]}]
  (fn [x] (when (or (not where) (-> x where-fn where)) (-> x event-fn event))))

(defn- observer
  [{:keys [live error close] :as callbacks} dispatch]
  (proxy [SubscriptionObserver] []
    (onLiveProcessingStart [_] (when live (live)))
    (onEvent [x _] (dispatch x))
    (onError [x] (when error (-> x error->map error)))
    (onClose [] (when close (close)))))

(defn- persistent-subscription-listener
  [{:keys [live error] :as callbacks} m]
  (let [dispatch (dispatch-fn callbacks :event-fn #(event-record->map % m)
                              :where-fn event-record-meta)]
    (actor (onReceive
             [x]
             (condp instance? x
               LiveProcessingStarted$ (when live (live))
               EventRecord (dispatch x)
               (if (and (instance? FSM$Event x)
                        (instance? Status$Failure (.event ^FSM$Event x))
                        error)
                 (let [^Status$Failure fail (.event ^FSM$Event x)]
                   (-> (.cause fail) error->map error))
                 (.unhandled ^Actor this x)))))))

(defn subscribe-to-stream
  "Subscribes to a single event stream. New events written to the stream while
  the subscription is active will be pushed to the client. If :from is
  specified, existing events :from onwards are read from the stream and
  presented to the user of callbacks as if they had been pushed. Once the end of
  the stream is read the subscription is transparently (to the user) switched to
  push new events as they are written. If events have already been received and
  resubscription from the same point is desired, use the event number of the
  last event processed which appeared on the subscription (incremented by 1).

  Options:
  :conn             - (required) see write-events fn
  :stream           - ”
  :from             - the starting event number (if omitted, live subscription
                      only)
  :resolve-link-tos - see read-event fn
  :format           - see write-events fn
  :meta-format      - ”
  :login            - ”
  :password         - ”

  Callbacks:
  :live  - called when subscription becomes live (no arguments)
  :event - (required) called with one argument which is the event received.
           please refer to read-event fn for an info about the received events.
  :where - called before :event with one argument which is the event metadata
           map. :event will not be called if the return value is falsy. please
           refer to read-event fn for an info about metadata map. the difference
           is that the value of :meta is bytes (use deserialize multimethod).
  :error - called with one argument which is the error - see write-events fn
  :close - called when subscription closes (no arguments)

  The return value can be used to close the subscription by calling the
  close-subscription fn or by using the Clojure with-open macro."
  [{:keys [^EsConnection conn stream from resolve-link-tos]
    :or   {resolve-link-tos false} :as m}
   {:keys [live event where error close] :as callbacks}]
  {:pre [(conn? conn) (stream? stream) (num?? from) (bool? resolve-link-tos)
         (fn?? live) (fn? event) (fn?? where) (fn?? error) (fn?? close)]}
  (let [o (observer callbacks (dispatch-fn callbacks :event-fn #(event->map % m)
                                           :where-fn event-meta))]
    (if from
      (.subscribeToStreamFrom conn stream o (when (pos? from) (dec from))
                              resolve-link-tos (creds m))
      (.subscribeToStream conn stream o resolve-link-tos (creds m)))))

(defn subscribe-to-all-streams
  "Subscribes to all events in the event store. New events written to the stream
  while the subscription is active will be pushed to the client. If :pos is
  specified, existing events after position :pos (excluding) are read from the
  event store and presented to the user of callbacks as if they had been pushed.
  Once the end of the stream is read the subscription is transparently (to the
  user) switched to push new events as they are written. If events have already
  been received and resubscription from the same point is desired, use the
  position representing the last event processed which appeared on the
  subscription.

  Options:
  :conn             - (required) see write-events fn
  :pos              - either an excluded position (a map with :commit and
                      :prepare keys) to start reading after or :zero to read all
                      events or omitted for live subscription only
  :resolve-link-tos - see read-event fn
  :format           - see write-events fn
  :meta-format      - ”
  :login            - ”
  :password         - ”

  Please refer to subscribe-to-stream fn for an info about callbacks. One extra
  metadata received with each event is:
  :pos - the event position which can be used as the :pos option to resubscribe
         (has :commit and :prepare subkeys, can be nil)

  The return value - see subscribe-to-stream fn."
  [{:keys [^EsConnection conn pos resolve-link-tos]
    :or   {resolve-link-tos false} :as m}
   {:keys [live event where error close] :as callbacks}]
  {:pre [(conn? conn) (or (not pos) (= pos :zero) (map? pos))
         (bool? resolve-link-tos) (fn?? live) (fn? event) (fn?? where)
         (fn?? error) (fn?? close)]}
  (let [o (observer callbacks (dispatch-fn callbacks
                                           :event-fn #(indexed-event->map % m)
                                           :where-fn indexed-event-meta))]
    (if pos
      (.subscribeToAllFrom conn o (when (map? pos) (map->pos pos))
                           resolve-link-tos (creds m))
      (.subscribeToAll conn o resolve-link-tos (creds m)))))

(defn close-subscription
  "Closes a subscription returned by the subscribe-to-stream,
  subscribe-to-all-streams or persistently-subscribe fns. Returns nil."
  [^Closeable sub]
  {:pre [(instance? Closeable sub)]}
  (.close sub))

(defn- write-result->map
  [^WriteResult wr]
  {:pos          (when wr (pos->map (.logPosition wr)))
   :next-exp-ver (when wr (.. wr nextExpectedVersion value))})

(defn set-stream-metadata
  "Sets the metadata for a stream.

  Options:
  :conn     - (required) see write-events fn
  :stream   - ”
  :exp-ver  - ”
  :format   - see write-events fn
  :login    - ”
  :password - ”

  The metadata can be anything (de)serializable.

  The return value - see write-events fn. The difference is that :next-exp-ver
  can be nil.

  Please note that the :exp-ver input and the :next-exp-ver output refer to the
  metadata stream as opposed to the stream itself."
  [{:keys [^EsConnection conn stream exp-ver format] :as m} metadata]
  {:pre [(conn? conn) (stream? stream) (exp-ver? exp-ver)]}
  (let [bytes (-> (serialize metadata format) (.. value toArray))]
    (-> (.setStreamMetadata conn stream (exp-ver->obj exp-ver) bytes (creds m))
        (->promise write-result->map))))

(defn get-stream-metadata
  "Reads the metadata for a stream.

  Options:
  :conn     - (required) see write-events fn
  :stream   - ”
  :format   - see write-events fn
  :login    - ”
  :password - ”

  The successful return value is a promise derefing to deserialized metadata.
  The failed return value - see write-events fn."
  [{:keys [^EsConnection conn stream format] :as m}]
  {:pre [(conn? conn) (stream? stream)]}
  (-> (.getStreamMetadataBytes conn stream (creds m))
      (->promise #(deserialize % format))))

(defn persistently-subscribe
  "Starts listening to a persistent subscription.

  Persistent Subscriptions (extract from the Event Store documentation):
  This kind of subscriptions supports the “competing consumers” messaging
  pattern. The subscription state is stored server side in the Event Store and
  allows for at-least-once delivery guarantees across multiple consumers on the
  same stream.
  It is possible to have many groups of consumers compete on the same stream,
  with each group getting an at-least-once guarantee.

  Options:
  :conn     - (required) see write-events fn
  :stream   - ”
  :group    - (required) the name of a group which should be created with the
              create-persistent-subscription fn first
  :auto-ack - if this is true (the default), an acknowledgement will be sent
              automatically on each event. alternatively, use the manual-ack fn.
  :login    - see write-events fn
  :password - ”

  Please refer to subscribe-to-stream fn for an info about callbacks noting that
  the :close callback does not exist here.

  The return value - see subscribe-to-stream fn.

  WARNING: uses a hack to get a private field from the JVM Client!"
  [{:keys [^EsConnection conn stream group auto-ack] :or {auto-ack true} :as m}
   {:keys [live event where error] :as callbacks}]
  {:pre [(conn? conn) (stream? stream) (string? group) (bool? auto-ack)
         (fn?? live) (fn? event) (fn?? where) (fn?? error)]}
  (let [listener (spawn conn (persistent-subscription-listener callbacks m))
        sub (PersistentSubscriptionActor/props
              (conn-actor conn) listener (EventStream/apply ^String stream)
              group (or (creds m) (Option/empty)) nil auto-ack)]
    (ActorCloseable. (spawn conn sub))))

(defn- persistent-subscription-settings
  [{:keys [resolve-link-tos from extra-stats timeout-ms max-retries live-buf-sz
           read-batch-sz hist-buf-sz checkpoint-after-ms min-checkpoints
           max-checkpoints max-subscribers strategy]}]
  {:pre [(bool?? resolve-link-tos) (num?? from) (bool?? extra-stats)
         (num?? timeout-ms) (num?? max-retries) (num?? live-buf-sz)
         (num?? read-batch-sz) (num?? hist-buf-sz) (num?? checkpoint-after-ms)
         (num?? min-checkpoints) (num?? max-checkpoints) (num?? max-subscribers)
         (or (not strategy) (#{:round-robin :dispatch-to-single} strategy))]}
  (-> (PersistentSubscriptionSettingsBuilder.)
      (cond-> (true? resolve-link-tos) (.resolveLinkTos))
      (cond-> (false? resolve-link-tos) (.doNotResolveLinkTos))
      (cond-> from (.startFrom ^long from))
      (cond-> (true? extra-stats) (.withExtraStatistic))
      (cond-> timeout-ms (.messageTimeout timeout-ms ms))
      (cond-> max-retries (.maxRetryCount max-retries))
      (cond-> live-buf-sz (.liveBufferSize live-buf-sz))
      (cond-> read-batch-sz (.readBatchSize read-batch-sz))
      (cond-> hist-buf-sz (.historyBufferSize hist-buf-sz))
      (cond-> checkpoint-after-ms (.checkPointAfter checkpoint-after-ms ms))
      (cond-> min-checkpoints (.minCheckPointCount min-checkpoints))
      (cond-> max-checkpoints (.maxCheckPointCount max-checkpoints))
      (cond-> max-subscribers (.maxSubscriberCount max-subscribers))
      (cond-> (= strategy :round-robin) (.roundRobin))
      (cond-> (= strategy :dispatch-to-single) (.dispatchToSingle))
      (.build)))

(defn create-persistent-subscription
  "Asynchronously creates a persistent subscription group on a stream.

  Options:
  :conn     - (required) see write-events fn
  :stream   - ”
  :group    - (required) the name of a group to create, e.g. \"foo\"
  :login    - see write-events fn
  :password - ”

  settings is a map with optional values for:
  :resolve-link-tos    - see read-event fn
  :from                - see subscribe-to-stream fn
  :extra-stats         - whether or not in depth latency statistics should be
                         tracked on this subscription (false by default)
  :timeout-ms          - the amount of time after which a message should be
                         considered to be timedout and retried (milliseconds,
                         30000 by default)
  :max-retries         - the maximum number of retries (due to timeout) before a
                         message get considered to be parked (500 by default)
  :live-buf-sz         - the size of the buffer listening to live messages as
                         they happen (500 by default)
  :read-batch-sz       - the number of events read at a time when paging in
                         history (10 by default)
  :hist-buf-sz         - the number of events to cache when paging through
                         history (20 by default)
  :checkpoint-after-ms - the amount of time to try to checkpoint after
                         (milliseconds, 2000 by default)
  :min-checkpoints     - the minimum number of messages to checkpoint (10 by
                         default)
  :max-checkpoints     - the maximum number of messages to checkpoint. if this
                         number is a reached a checkpoint will be forced.
                         (1000 by default)
  :max-subscribers     - the maximum number of subscribers allowed (default: 0)
  :strategy            - the strategy to use for distributing events to client
                         consumers:
                         -   :round-robin (default)
                         distributes events to each client in a round robin
                         fashion
                         -   :dispatch-to-single
                         distributes events to a single client until it is full.
                         then round robin to the next client.

  Returns a promise which derefs to :done on success. The failed return value -
  see write-events fn."
  [{:keys [^EsConnection conn stream group] :as m} settings]
  {:pre [(conn? conn) (stream? stream) (string? group)]}
  (-> (.createPersistentSubscription
        conn stream group (persistent-subscription-settings settings) (creds m))
      (->promise)))

(defn update-persistent-subscription
  "Asynchronously updates a persistent subscription group on a stream. Please
  refer to create-persistent-subscription fn for more info."
  [{:keys [^EsConnection conn stream group] :as m} settings]
  {:pre [(conn? conn) (stream? stream) (string? group)]}
  (-> (.updatePersistentSubscription
        conn stream group (persistent-subscription-settings settings) (creds m))
      (->promise)))

(defn delete-persistent-subscription
  "Asynchronously deletes a persistent subscription group on a stream. Please
  refer to create-persistent-subscription fn for more info."
  [{:keys [^EsConnection conn stream group] :as m}]
  {:pre [(conn? conn) (stream? stream) (string? group)]}
  (-> (.deletePersistentSubscription conn stream group (creds m)) (->promise)))

(defn- ^UUID event-id [event] {:pre [event]} (-> event meta :id))

(defn manual-ack
  "Sends an acknowledgement for an event. sub should be a return value of the
  persistently-subscribe fn. Returns nil."
  [^ActorCloseable sub event]
  {:pre [(instance? ActorCloseable sub) (event-id event)]}
  (let [actor (.actor sub)
        msg (PersistentSubscriptionActor$ManualAck. (event-id event))]
    (.tell actor msg actor)))
