(ns clj-kafka-x.data-test
  (:require [clojure.test :refer :all]
            [clj-kafka-x.data :refer :all])
  (:import [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer OffsetAndMetadata]
           [java.util Properties]))

(deftest test-map->properties
  (testing "map->properties converts map to Properties"
    (let [m {"key1" "value1" "key2" "value2"}
          props (map->properties m)]
      (is (instance? Properties props))
      (is (= "value1" (.getProperty props "key1")))
      (is (= "value2" (.getProperty props "key2"))))))

(deftest test-map->topic-partition
  (testing "map->topic-partition converts map to TopicPartition"
    (let [m {:topic "test-topic" :partition 1}
          tp (map->topic-partition m)]
      (is (instance? TopicPartition tp))
      (is (= "test-topic" (.topic tp)))
      (is (= 1 (.partition tp)))))
  
  (testing "map->topic-partition throws exception on invalid input"
    (is (thrown? clojure.lang.ExceptionInfo (map->topic-partition {:topic "test-topic"})))
    (is (thrown? clojure.lang.ExceptionInfo (map->topic-partition {:partition 1})))))

(deftest test-map->offset-metadata
  (testing "map->offset-metadata converts map to OffsetAndMetadata"
    (let [m {:offset 100 :metadata "test-metadata"}
          om (map->offset-metadata m)]
      (is (instance? OffsetAndMetadata om))
      (is (= 100 (.offset om)))
      (is (= "test-metadata" (.metadata om)))))
  
  (testing "map->offset-metadata throws exception on invalid input"
    (is (thrown? clojure.lang.ExceptionInfo (map->offset-metadata {:offset 100})))
    (is (thrown? clojure.lang.ExceptionInfo (map->offset-metadata {:metadata "test"})))))