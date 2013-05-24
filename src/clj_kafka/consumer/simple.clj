(ns clj-kafka.consumer.simple
  (:use [clj-kafka.core :only (to-clojure)])
  (:import [kafka.javaapi.consumer SimpleConsumer]
           [kafka.api FetchRequest OffsetRequest]))

(defn consumer
  "Create a consumer to connect to host and port. Port will
   normally be 9092."
  [host port & {:keys [timeout buffer-size] :or {timeout 100000 buffer-size 10000}}]
  (SimpleConsumer. host port timeout buffer-size))

(defn earliest-offset
  "Retrieves the earliest offset available for topic and partition."
  [^SimpleConsumer consumer topic partition]
  (long (first (.getOffsetsBefore consumer topic partition (OffsetRequest/EarliestTime) 1))))

(defn latest-offsets
  "Retrieves n most recent offsets for topic and partition."
  [^SimpleConsumer consumer topic partition n]
  (map long (.getOffsetsBefore consumer topic partition (OffsetRequest/LatestTime) n)))

(defn max-offset
  [consumer topic partition]
  (first (latest-offsets consumer topic partition 1)))

(defn fetch
  "Creates a request to retrieve a set of messages from the
   specified topic.

   Arguments:
   partition: as specified when producing messages
   offset: offset to start retrieval
   max-size: number of bytes to retrieve"
  [topic partition offset max-size]
  (FetchRequest. topic partition offset max-size))

(defn messages
  "Creates a sequence of messages from the given request."
  [^SimpleConsumer consumer request]
  (map to-clojure (iterator-seq (.iterator (.fetch consumer request)))))
