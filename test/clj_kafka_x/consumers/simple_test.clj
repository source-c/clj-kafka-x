(ns clj-kafka-x.consumers.simple-test
  (:require [clojure.test :refer :all]
            [clj-kafka-x.consumers.simple :as kc])
  (:import [org.apache.kafka.common.serialization StringDeserializer ByteArrayDeserializer]))

(deftest test-deserializers
  (testing "string-deserializer creates a StringDeserializer"
    (is (instance? StringDeserializer (kc/string-deserializer))))
  
  (testing "byte-array-deserializer creates a ByteArrayDeserializer"
    (is (instance? ByteArrayDeserializer (kc/byte-array-deserializer)))))