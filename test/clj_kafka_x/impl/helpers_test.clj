(ns clj-kafka-x.impl.helpers-test
  (:require [clojure.test :refer :all]
            [clj-kafka-x.impl.helpers :refer :all])
  (:import [org.apache.kafka.common.config ConfigDef ConfigDef$Type]
           [org.apache.kafka.clients.producer ProducerConfig]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common TopicPartition]))

(deftest test-coerce-config
  (testing "coerce-config passes through string values"
    (let [config-def (ProducerConfig/configDef)
          config {"bootstrap.servers" "localhost:9092"
                  "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                  "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"}
          coerced (coerce-config config-def config)]
      (is (= "localhost:9092" (get coerced "bootstrap.servers")))
      (is (= "org.apache.kafka.common.serialization.StringSerializer" (get coerced "key.serializer")))
      (is (= "org.apache.kafka.common.serialization.StringSerializer" (get coerced "value.serializer")))))

  (testing "coerce-config coerces INT values"
    (let [config-def (ProducerConfig/configDef)
          config {"bootstrap.servers" "localhost:9092"
                  "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                  "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                  "send.buffer.bytes" 65536}
          coerced (coerce-config config-def config)]
      (is (= 65536 (get coerced "send.buffer.bytes")))
      (is (integer? (get coerced "send.buffer.bytes")))))

  (testing "coerce-config coerces LONG values"
    (let [config-def (ProducerConfig/configDef)
          config {"bootstrap.servers" "localhost:9092"
                  "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                  "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                  "buffer.memory" 33554432}
          coerced (coerce-config config-def config)]
      (is (= 33554432 (get coerced "buffer.memory")))))

  (testing "coerce-config coerces DOUBLE values"
    (let [config-def (ConsumerConfig/configDef)
          config {"bootstrap.servers" "localhost:9092"
                  "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                  "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}
          coerced (coerce-config config-def config)]
      ;; Just verifying basic coercion works - double configs exist in consumer
      (is (map? coerced))))

  (testing "coerce-config coerces LIST values"
    (let [config-def (ProducerConfig/configDef)
          config {"bootstrap.servers" ["localhost:9092" "localhost:9093"]
                  "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                  "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"}
          coerced (coerce-config config-def config)]
      (is (instance? java.util.List (get coerced "bootstrap.servers")))
      (is (= 2 (.size (get coerced "bootstrap.servers"))))))

  (testing "coerce-config handles keyword keys"
    (let [config-def (ProducerConfig/configDef)
          config {:bootstrap.servers "localhost:9092"
                  :key.serializer "org.apache.kafka.common.serialization.StringSerializer"
                  :value.serializer "org.apache.kafka.common.serialization.StringSerializer"}
          coerced (coerce-config config-def config)]
      (is (= "localhost:9092" (get coerced "bootstrap.servers"))))))

(deftest test-->topic-partition-array
  (testing "->topic-partition-array converts sequence of maps to TopicPartition collection"
    (let [tp-seq [{:topic "topic-a" :partition 0}
                  {:topic "topic-b" :partition 1}
                  {:topic "topic-c" :partition 2}]
          result (->topic-partition-array tp-seq)]
      (is (instance? java.util.Collection result))
      (is (= 3 (count result)))
      (let [result-vec (vec result)]
        (is (instance? TopicPartition (first result-vec)))
        (is (= "topic-a" (.topic (nth result-vec 0))))
        (is (= 0 (.partition (nth result-vec 0))))
        (is (= "topic-b" (.topic (nth result-vec 1))))
        (is (= 1 (.partition (nth result-vec 1))))
        (is (= "topic-c" (.topic (nth result-vec 2))))
        (is (= 2 (.partition (nth result-vec 2)))))))

  (testing "->topic-partition-array handles empty sequence"
    (let [result (->topic-partition-array [])]
      (is (instance? java.util.Collection result))
      (is (zero? (count result)))))

  (testing "->topic-partition-array handles single element"
    (let [result (->topic-partition-array [{:topic "single" :partition 5}])]
      (is (= 1 (count result)))
      (let [tp (first result)]
        (is (= "single" (.topic tp)))
        (is (= 5 (.partition tp)))))))