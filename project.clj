(defproject net.tbt-post/clj-kafka-x "0.5.1"
  :description "A Clojure wrapper for Apache Kafka v2 client"
  :url "https://github.com/source-c/clj-kafka-x"
  :license {:name "Apache License 2.0"
            :url  "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.apache.kafka/kafka_2.12 "3.0.0"
                  :exclusions [javax.jms/jms
                               com.sun.jdmk/jmxtools
                               com.sun.jmx/jmxri
                               org.slf4j/slf4j-log4j12]]
                 [org.apache.zookeeper/zookeeper "3.7.0"
                  :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.apache.kafka/kafka-clients "3.0.0"]])
