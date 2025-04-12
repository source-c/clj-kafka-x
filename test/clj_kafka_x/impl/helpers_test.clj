(ns clj-kafka-x.impl.helpers-test
  (:require [clojure.test :refer :all]
            [clj-kafka-x.impl.helpers :refer :all])
  (:import [org.apache.kafka.common.config ConfigDef ConfigDef$Type]
           [org.apache.kafka.clients.producer ProducerConfig]))

(deftest test-coerce-config
  (testing "coerce-config passes through string values"
    (let [config-def (ProducerConfig/configDef)
          config {"bootstrap.servers" "localhost:9092"
                  "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                  "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"}
          coerced (coerce-config config-def config)]
      
      (is (= "localhost:9092" (get coerced "bootstrap.servers")))
      (is (= "org.apache.kafka.common.serialization.StringSerializer" (get coerced "key.serializer")))
      (is (= "org.apache.kafka.common.serialization.StringSerializer" (get coerced "value.serializer"))))))