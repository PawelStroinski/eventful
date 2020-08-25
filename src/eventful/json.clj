(ns eventful.json
  (:require [eventful.core :refer [serialize deserialize]]
            [cheshire.core :refer [generate-stream parse-stream]]
            [clojure.java.io :as io])
  (:import (eventstore.core Content ContentType$Json$ ByteString)
           (java.io ByteArrayOutputStream)))

(defmethod serialize :json
  [x format]
  (with-open [s (ByteArrayOutputStream. 4096)
              w (io/writer s)]
    (generate-stream x w)
    (-> (.toByteArray s)
        (ByteString/apply)
        (Content/apply (ContentType$Json$.)))))

(defmethod deserialize :json
  [bytes format]
  (with-open [r (io/reader bytes)]
    (parse-stream r)))
