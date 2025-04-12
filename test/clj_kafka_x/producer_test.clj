(ns clj-kafka-x.producer-test
  (:require [clojure.test :refer :all]
            [clj-kafka-x.producer :as kp])
  (:import [org.apache.kafka.common.serialization StringSerializer ByteArraySerializer]
           [org.apache.kafka.clients.producer ProducerRecord]))

(deftest test-serializers
  (testing "string-serializer creates a StringSerializer"
    (is (instance? StringSerializer (kp/string-serializer))))
  
  (testing "byte-array-serializer creates a ByteArraySerializer"
    (is (instance? ByteArraySerializer (kp/byte-array-serializer)))))

(deftest test-record-creation
  (testing "record creates ProducerRecord with only topic and value"
    (let [record (kp/record "test-topic" "test-value")]
      (is (instance? ProducerRecord record))
      (is (= "test-topic" (.topic record)))
      (is (= "test-value" (.value record)))
      (is (nil? (.key record)))))
  
  (testing "record creates ProducerRecord with topic, key, and value"
    (let [record (kp/record "test-topic" "test-key" "test-value")]
      (is (instance? ProducerRecord record))
      (is (= "test-topic" (.topic record)))
      (is (= "test-key" (.key record)))
      (is (= "test-value" (.value record)))))
  
  (testing "record creates ProducerRecord with topic, partition, key, and value"
    (let [record (kp/record "test-topic" (int 1) "test-key" "test-value")]
      (is (instance? ProducerRecord record))
      (is (= "test-topic" (.topic record)))
      (is (= 1 (.partition record)))
      (is (= "test-key" (.key record)))
      (is (= "test-value" (.value record))))))