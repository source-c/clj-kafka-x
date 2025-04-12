(ns clj-kafka-x.test-helpers
  (:require [clojure.test :refer :all]
            [clj-kafka-x.producer :as kp]
            [clj-kafka-x.consumers.simple :as kc])
  (:import [org.apache.kafka.clients.producer MockProducer]
           [org.apache.kafka.clients.consumer MockConsumer OffsetResetStrategy]
           [org.apache.kafka.common.serialization StringSerializer StringDeserializer]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.producer ProducerConfig]))

;; We need to explicitly import the inner class because of a limitation in Clojure's interop
(def earliest-offset-reset-strategy (OffsetResetStrategy/EARLIEST))

(defn create-mock-producer
  "Creates a MockProducer for testing"
  []
  ;; Using constructor with 3 args: autoComplete, keySerializer, valueSerializer
  (doto (MockProducer.)
    (.init true 
           (StringSerializer.)
           (StringSerializer.))))

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