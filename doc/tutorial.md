# A Quick but Eventful Tutorial

## Getting Started

After the [Event Store](https://eventstore.org/) is successfully installed and
started, the obvious next step is to connect to it:

```clojure
(use 'eventful.core)
=> nil
(def conn (connect {:hostname "127.0.0.1" :port 1113
                    :login    "admin"     :password "changeit"}))
=> #'user/conn
```

## Writing Events

Now we can write the first two events in this tutorial (there will be more):

```clojure
(write-events {:conn conn :stream "inventory-item-1" :exp-ver :no-stream}
              {:event :created :name "foo"}
              {:event :items-checked-in :count 100})
=>
#object[clojure.core$promise$reify__7005
        0x3a3d3cb5
        {:status :pending, :val nil}]
```

Let's investigate if it all worked out:

```clojure
@*1
=> {:next-exp-ver 1, :pos {:commit 16796, :prepare 16796}}
```

We can use the `:next-exp-ver` above to add another event:

```clojure
(write-events {:conn conn :stream "inventory-item-1" :exp-ver 1}
              {:event :renamed :new-name "bar"})
=>
#object[clojure.core$promise$reify__7005
        0x21de42ec
        {:status :pending, :val nil}]
@*1
=> {:next-exp-ver 2, :pos {:commit 46786, :prepare 46786}}
```

## Transactions

Transactions are supported as well. Maybe adding three more events is not such a
bad idea:

```clojure
(def tx @(tx-start {:conn conn :stream "inventory-item-1" :exp-ver 2}))
=> #'user/tx
@(tx-write-events {:tx tx}
                  {:event :items-removed :count 1}
                  {:event :items-removed :count 2})
=> :done
@(tx-write-events {:tx tx}
                  {:event :items-removed :count 3})
=> :done
@(tx-commit tx)
=> :done
```

## Reading Events

Let's verify that all events are really there:

```clojure
(->> (for [num (range 6)]
       (read-event {:conn conn :stream "inventory-item-1"} num))
     (map deref))
=>
({:event :created, :name "foo"}
 {:event :items-checked-in, :count 100}
 {:event :renamed, :new-name "bar"}
 {:event :items-removed, :count 1}
 {:event :items-removed, :count 2}
 {:event :items-removed, :count 3})
```

Alternatively, we can get the events in one call. Let's get the first 5 events:

```clojure
@(read-stream {:conn conn :stream "inventory-item-1"} [nil 5])
=>
[{:event :created, :name "foo"}
 {:event :items-checked-in, :count 100}
 {:event :renamed, :new-name "bar"}
 {:event :items-removed, :count 1}
 {:event :items-removed, :count 2}]
```

Changing `5` to `-5` returns the last 5 events in reverse order:

```clojure
@(read-stream {:conn conn :stream "inventory-item-1"} [nil -5])
=>
[{:event :items-removed, :count 3}
 {:event :items-removed, :count 2}
 {:event :items-removed, :count 1}
 {:event :renamed, :new-name "bar"}
 {:event :items-checked-in, :count 100}]
```

Metadata is returned with each event:

```clojure
(-> *1 first meta)
=>
{:id #uuid"3c2034b4-71cf-48d6-a17f-98ef9ddb1653",
 :type "event",
 :num 5,
 :stream "inventory-item-1",
 :date #object[org.joda.time.DateTime 0x56009895 "2018-01-10T19:51:48.554Z"]}
```

## Subscriptions

With the basics out of the way, we can now look at subscribing to a stream:

```clojure
(subscribe-to-stream {:conn conn :stream "inventory-item-1" :from 0}
                     {:event (partial prn "Received:")})
=>
#object[eventstore.util.ActorCloseable
        0x444c0538
        "ActorCloseable(Actor[akka://Eventful/user/$c#-537978204])"]
Received: {:event :created, :name foo}
Received: {:event :items-checked-in, :count 100}
Received: {:event :renamed, :new-name bar}
Received: {:event :items-removed, :count 1}
Received: {:event :items-removed, :count 2}
Received: {:event :items-removed, :count 3}
```

We should receive an event as soon as it is written (but not sooner):

```clojure
@(write-events {:conn conn :stream "inventory-item-1" :exp-ver 5}
               {:event :items-removed :count 4})
=> {:next-exp-ver 6, :pos {:commit 137294, :prepare 137294}}
Received: {:event :items-removed, :count 4}
```

Now we can close our subscription:

```clojure
(close-subscription *2)
=> nil
```

## Persistent Subscriptions

Thanks to our persistence we advanced to the [persistent subscriptions]
(eventful.core.html#var-persistently-subscribe):
 
```clojure
@(create-persistent-subscription
   {:conn conn :stream "inventory-item-1" :group "baz"} {:from 0})
=> :done
(persistently-subscribe {:conn conn :stream "inventory-item-1" :group "baz"}
                        {:event (partial prn "Persistently received:")})
=>
#object[akka.actor.RepointableActorRef
        0x2812b316
        "Actor[akka://Eventful/user/$f#1630272201]"]
Persistently received: {:event :created, :name foo}
Persistently received: {:event :items-checked-in, :count 100}
Persistently received: {:event :renamed, :new-name bar}
Persistently received: {:event :items-removed, :count 1}
Persistently received: {:event :items-removed, :count 2}
Persistently received: {:event :items-removed, :count 3}
Persistently received: {:event :items-removed, :count 4}
```

This time too we should receive an event as soon as it is written. No surprise
there:

```clojure
@(write-events {:conn conn :stream "inventory-item-1" :exp-ver 6}
               {:event :items-removed :count 5})
=> {:next-exp-ver 7, :pos {:commit 182729, :prepare 182729}}
Persistently received: {:event :items-removed, :count 5}
```

Let's close this subscription (for now):

```clojure
(close-subscription *2)
=> nil
```

If we add another event and reopen our persistent subscription, we will receive
it. Also, let's find out how to use the manual ACK as opposed to the default
automatic ACK:

```clojure
@(write-events {:conn conn :stream "inventory-item-1" :exp-ver 7}
               {:event :items-removed :count 6})
=> {:next-exp-ver 8, :pos {:commit 197943, :prepare 197943}}
(declare sub)
=> #'user/sub
(def sub (persistently-subscribe
           {:conn conn :stream "inventory-item-1" :group "baz" :auto-ack false}
           {:event (fn [event]
                     (prn "Persistently received (2):" event)
                     (manual-ack sub event))}))
=> #'user/sub
Persistently received (2): {:event :items-removed, :count 6}
```

Finally let's close this subscription so it does not print any more events:

```clojure
(close-subscription sub)
=> nil
```

## JSON

By default Eventful will write & read events in EDN. To use JSON instead, we
need to do few things:

1. Include the cheshire dependency in our project.
2. Require the `eventful.json` namespace.
3. Use the `:format` option when writing & reading.

I'll leave the first step for you. Go on and add the dependency then restart
your REPL and reconnect to the Event Store. The remaining two steps are:

```clojure
(require 'eventful.json)
=> nil
@(write-events {:conn conn :stream "inventory-item-1" :exp-ver 8 :format :json}
               {:event :items-removed :count 7})
=> {:next-exp-ver 9, :pos {:commit 1228589, :prepare 1228589}}
@(read-event {:conn conn :stream "inventory-item-1" :format :json} 9)
=> {"event" "items-removed", "count" 7}
```

## Further Events

We wrote a total of 10 events in this tutorial! Didn't we say it will be
Eventful? But that's not all. To write even more, check out the
[API Docs](eventful.core.html)! Happy eventing!
