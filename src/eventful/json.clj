(ns eventful.json
  (:require [eventful.core :refer [serialize deserialize]]
            [cheshire.core :refer [generate-stream parse-stream]]
            [clojure.java.io :as io])
  (:import (java.io ByteArrayOutputStream)
           (eventstore Content ContentType$Json$)
           (akka.util ByteStringBuilder)))

(defn- ->bytes
  [x]
  (let [s (ByteArrayOutputStream.)]
    (with-open [w (io/writer s)]
      (generate-stream x w))
    (.toByteArray s)))

(defmethod serialize :json
  [x format]
  (let [builder (ByteStringBuilder.)]
    (.putBytes builder (->bytes x))
    (Content/apply (.result builder) (ContentType$Json$.))))

(defmethod deserialize :json
  [bytes format]
  (with-open [r (io/reader bytes)]
    (parse-stream r)))
