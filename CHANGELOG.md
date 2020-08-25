## 1.0.0 - 2020-08-25

* Updated dependencies
* Removed alpha status
* Switched back to EDN as the default serializer because of this note from Transit documentation: *Transit is intended primarily as a wire protocol for transferring data between applications. If storing Transit data durably, readers and writers are expected to use the same version of Transit and you are responsible for migrating/transforming/re-storing that data when and if the transit format changes.*
* Updated mini tutorial

## 0.1.0-alpha6 - 2019-05-23

* Updated serializers. Added Transit-JSON as the new default.
* Cleared-up returning nil positions.
* Fixed reading nil optionals.
* Fixed documentation for persistently-subscribe.
* Updated dependencies

## 0.1.0-alpha5 - 2018-09-11

* Updated dependencies
* Added a new stress test for subscribe-to-all-streams

## 0.1.0-alpha4 - 2018-06-01

* Added metadata to reduce-stream fn result
* Renamed (un)wrap-event fns to (un)wrap

## 0.1.0-alpha3 - 2018-05-25

* Added reduce-stream fn built on top of read-stream fn

## 0.1.0-alpha2 - 2018-05-24

* Replaced Clojure promises with Manifold deferreds

## 0.1.0-alpha - 2018-01-23

* The initial release
