(ns eventful.transit
  (:require [eventful.core :refer [serialize deserialize]]
            [cognitect.transit :as t])
  (:import (eventstore.core Content ContentType$Json$ ByteString)
           (java.io ByteArrayOutputStream ByteArrayInputStream)))

(defmethod serialize :transit
  [x format]
  (with-open [s (ByteArrayOutputStream. 4096)]
    (let [w (t/writer s :json)]
      (t/write w x))
    (-> (.toByteArray s)
      (ByteString/apply)
      (Content/apply (ContentType$Json$.)))))

(defmethod deserialize :transit
  [bytes format]
  (with-open [s (ByteArrayInputStream. bytes)]
    (let [reader (t/reader s :json)]
      (t/read reader))))
