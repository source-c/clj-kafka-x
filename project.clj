(defproject net.tbt-post/clj-kafka-x "0.8.0"
  :description "A Clojure wrapper for Apache Kafka v2/v3 client"
  :url "https://github.com/source-c/clj-kafka-x"
  :license {:name "Apache License 2.0"
            :url  "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.apache.kafka/kafka_2.12 "3.9.0"
                  :exclusions [com.sun.jdmk/jmxtools
                               com.sun.jmx/jmxri
                               org.slf4j/slf4j-log4j12]]
                 [org.apache.kafka/kafka-clients "4.0.0"]])
