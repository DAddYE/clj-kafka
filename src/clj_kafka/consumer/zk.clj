(ns clj-kafka.consumer.zk
  (:import [kafka.consumer ConsumerConfig Consumer])
  (:use [clj-kafka.core :only (as-properties to-clojure with-resource pipe)])
  (:require [zookeeper :as zk]))

(defn consumer
  "Uses information in Zookeeper to connect to Kafka. More info on settings
   is available here: http://incubator.apache.org/kafka/configuration.html

   Recommended for using with with-resource:
   (with-resource [c (consumer m)]
     shutdown
     (take 5 (messages c \"test\")))
  
   Keys:
   zk.connect             : host:port for Zookeeper. e.g: 127.0.0.1:2181
   groupid                : consumer group. e.g. group1
   zk.sessiontimeout.ms   : session timeout. e.g. 400
   zk.synctime.ms         : Max time for how far a ZK follower can be behind a ZK leader. 200 ms
   autocommit.interval.ms : the frequency that the consumed offsets are committed to zookeeper.
   autocommit.enable      : if set to true, the consumer periodically commits to zookeeper the latest consumed offset of each partition"
  [m]
  (let [config (ConsumerConfig. (as-properties m))]
    (Consumer/createJavaConsumerConnector config)))

(defn shutdown
  "Closes the connection to Zookeeper and stops consuming messages."
  [^kafka.javaapi.consumer.ConsumerConnector consumer]
  (.shutdown consumer))

(defn- topic-map
  [topics]
  (apply hash-map (interleave topics
                              (repeat (Integer/valueOf 1)))))

(defn messages
  "Creates a sequence of messages from the given topics."
  [^kafka.javaapi.consumer.ConsumerConnector consumer & topics]
  (let [[queue-seq queue-put] (pipe)]
    (doseq [[topic streams] (.createMessageStreams consumer (topic-map topics))]
      (future (doseq [msg (iterator-seq (.iterator ^kafka.consumer.KafkaMessageStream (first streams)))]
                (queue-put (-> msg to-clojure (assoc :topic topic))))))
    queue-seq))

(defn topics
  "Connects to Zookeeper to read the list of topics. Use the same config
   as with consumer, uses the zk.connect value to connect to the client"
  [config]
  (with-resource [z (zk/connect (get config "zk.connect"))]
    zk/close 
    (sort (zk/children z "/brokers/topics"))))
