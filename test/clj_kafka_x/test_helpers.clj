(ns clj-kafka-x.test-helpers
  (:require [clojure.test :refer :all]
            [clj-kafka-x.producer :as kp]
            [clj-kafka-x.consumers.simple :as kc])
  (:import [org.apache.kafka.clients.producer MockProducer Partitioner]
           [org.apache.kafka.clients.consumer MockConsumer OffsetResetStrategy]
           [org.apache.kafka.common.serialization StringSerializer StringDeserializer]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.producer ProducerConfig]))

;; We need to explicitly import the inner class because of a limitation in Clojure's interop
(def earliest-offset-reset-strategy (OffsetResetStrategy/EARLIEST))

(defn- round-robin-partitioner
  "Creates a simple round-robin partitioner for testing"
  []
  (let [counter (atom 0)]
    (reify Partitioner
      (partition [_ topic key key-bytes value value-bytes cluster]
        (let [partitions (.partitionsForTopic cluster topic)
              num-partitions (count partitions)]
          (if (pos? num-partitions)
            (mod (swap! counter inc) num-partitions)
            0)))
      (close [_])
      (configure [_ configs]))))

(defn create-mock-producer
  "Creates a MockProducer for testing"
  []
  (MockProducer. true (round-robin-partitioner) (StringSerializer.) (StringSerializer.)))

(defn create-mock-consumer
  "Creates a MockConsumer for testing"
  []
  (MockConsumer. earliest-offset-reset-strategy))

(defn setup-consumer-assignment
  "Sets up a mock consumer with assignments for testing"
  [consumer topic partitions]
  (let [topic-partitions (map #(TopicPartition. topic %) partitions)]
    (.assign consumer topic-partitions)
    (.updateBeginningOffsets consumer 
                            (into {} (map #(vector % 0) topic-partitions)))
    consumer))

(defn add-consumer-record
  "Adds a record to the mock consumer"
  [consumer topic partition offset key value]
  (let [tp (TopicPartition. topic partition)]
    (.addRecord consumer (org.apache.kafka.clients.consumer.ConsumerRecord.
                           topic
                           partition
                           offset
                           key
                           value))
    consumer))