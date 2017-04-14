(defproject net.tbt-post/clj-kafka-x "0.1.0-SNAPSHOT"
  :description "A Clojure wrapper for Apache Kafka 0.10.x.x client"
  :url "https://github.com/source-c/clj-kafka-x"
  :license {:name "Apache License 2.0"
            :url  "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 ;; Kafka
                 [org.apache.kafka/kafka_2.11 "0.10.0.1"
                  :exclusions [javax.jms/jms
                               com.sun.jdmk/jmxtools
                               com.sun.jmx/jmxri
                               org.slf4j/slf4j-log4j12]]
                 [org.apache.zookeeper/zookeeper "3.4.6"
                  :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.apache.kafka/kafka-clients "0.10.0.1"]
                 [midje "1.8.3"]]
  :profiles {:docs {:plugins [[lein-pprint "1.1.2"]
                              [lein-codox "0.10.3"]
                              [org.timmc/nephila "0.3.0"]]}})
