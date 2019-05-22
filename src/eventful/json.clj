(ns eventful.json
  (:require [eventful.core :refer [serialize deserialize]]
            [cheshire.core :refer [generate-stream parse-stream]]
            [clojure.java.io :as io])
  (:import (eventstore Content ContentType$Json$)
           (akka.util ByteStringBuilder)))

(defmethod serialize :json
  [x format]
  (let [builder (ByteStringBuilder.)]
    (with-open [s (.asOutputStream builder)
                w (io/writer s)]
      (generate-stream x w))
    (Content/apply (.result builder) (ContentType$Json$.))))

(defmethod deserialize :json
  [bytes format]
  (with-open [r (io/reader bytes)]
    (parse-stream r)))
