(ns clj-kafka-x.data-test
  (:require [clojure.test :refer :all]
            [clj-kafka-x.data :refer :all])
  (:import [org.apache.kafka.common TopicPartition Node PartitionInfo Metric MetricName]
           [org.apache.kafka.clients.consumer ConsumerRecord OffsetAndMetadata]
           [org.apache.kafka.clients.producer RecordMetadata]
           [java.util Properties HashMap ArrayList]))

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

;; ToClojure protocol tests

(deftest test-to-clojure-nil
  (testing "to-clojure returns nil for nil input"
    (is (nil? (to-clojure nil)))))

(deftest test-to-clojure-topic-partition
  (testing "to-clojure converts TopicPartition to map"
    (let [tp (TopicPartition. "test-topic" 5)
          result (to-clojure tp)]
      (is (= {:topic "test-topic" :partition 5} result)))))

(deftest test-to-clojure-consumer-record
  (testing "to-clojure converts ConsumerRecord to map"
    (let [record (ConsumerRecord. "test-topic" 2 100 "test-key" "test-value")
          result (to-clojure record)]
      (is (= "test-topic" (:topic result)))
      (is (= 2 (:partition result)))
      (is (= 100 (:offset result)))
      (is (= "test-key" (:key result)))
      (is (= "test-value" (:value result)))))

  (testing "to-clojure handles ConsumerRecord with nil key"
    (let [record (ConsumerRecord. "test-topic" 0 0 nil "value-only")
          result (to-clojure record)]
      (is (nil? (:key result)))
      (is (= "value-only" (:value result))))))

(deftest test-to-clojure-offset-and-metadata
  (testing "to-clojure converts OffsetAndMetadata to map"
    (let [om (OffsetAndMetadata. 42 "some-metadata")
          result (to-clojure om)]
      (is (= {:offset 42 :metadata "some-metadata"} result))))

  (testing "to-clojure handles OffsetAndMetadata with empty metadata"
    (let [om (OffsetAndMetadata. 0 "")
          result (to-clojure om)]
      (is (= {:offset 0 :metadata ""} result)))))

(deftest test-to-clojure-node
  (testing "to-clojure converts Node to map"
    (let [node (Node. 1 "localhost" 9092)
          result (to-clojure node)]
      (is (= {:id 1 :host "localhost" :port 9092} result)))))

(deftest test-to-clojure-partition-info
  (testing "to-clojure converts PartitionInfo to map"
    (let [leader (Node. 1 "host1" 9092)
          replica1 (Node. 1 "host1" 9092)
          replica2 (Node. 2 "host2" 9093)
          replicas (into-array Node [replica1 replica2])
          isr (into-array Node [replica1])
          pi (PartitionInfo. "test-topic" 0 leader replicas isr)
          result (to-clojure pi)]
      (is (= "test-topic" (:topic result)))
      (is (= 0 (:partition result)))
      (is (= {:id 1 :host "host1" :port 9092} (:leader result)))
      (is (= [{:id 1 :host "host1" :port 9092} {:id 2 :host "host2" :port 9093}] (:replicas result)))
      (is (= [{:id 1 :host "host1" :port 9092}] (:in-sync-replicas result))))))

;; Conversion function tests

(deftest test-topic-partition-offsets->clj
  (testing "topic-partition-offsets->clj converts Java map to Clojure map"
    (let [java-map (HashMap.)
          tp1 (TopicPartition. "topic-a" 0)
          tp2 (TopicPartition. "topic-b" 1)
          om1 (OffsetAndMetadata. 100 "meta1")
          om2 (OffsetAndMetadata. 200 "meta2")
          _ (.put java-map tp1 om1)
          _ (.put java-map tp2 om2)
          result (topic-partition-offsets->clj java-map)]
      (is (= {:offset 100 :metadata "meta1"} (get result {:topic "topic-a" :partition 0})))
      (is (= {:offset 200 :metadata "meta2"} (get result {:topic "topic-b" :partition 1})))))

  (testing "topic-partition-offsets->clj handles empty map"
    (let [java-map (HashMap.)
          result (topic-partition-offsets->clj java-map)]
      (is (empty? result)))))

(deftest test-clj->topic-partition-offsets-map
  (testing "clj->topic-partition-offsets-map converts Clojure map to Java map"
    (let [clj-map {{:topic "topic-a" :partition 0} {:offset 100 :metadata "meta1"}
                   {:topic "topic-b" :partition 1} {:offset 200 :metadata "meta2"}}
          result (clj->topic-partition-offsets-map clj-map)]
      (is (instance? java.util.Map result))
      (is (= 2 (.size result)))
      (let [tp1 (TopicPartition. "topic-a" 0)
            tp2 (TopicPartition. "topic-b" 1)]
        (is (= 100 (.offset (.get result tp1))))
        (is (= "meta1" (.metadata (.get result tp1))))
        (is (= 200 (.offset (.get result tp2))))
        (is (= "meta2" (.metadata (.get result tp2)))))))

  (testing "clj->topic-partition-offsets-map handles empty map"
    (let [result (clj->topic-partition-offsets-map {})]
      (is (instance? java.util.Map result))
      (is (zero? (.size result))))))

(deftest test-topic-partitions-info->clj
  (testing "topic-partitions-info->clj converts Java map to Clojure map"
    (let [leader (Node. 1 "host1" 9092)
          replicas (into-array Node [leader])
          isr (into-array Node [leader])
          pi1 (PartitionInfo. "topic-a" 0 leader replicas isr)
          pi2 (PartitionInfo. "topic-a" 1 leader replicas isr)
          pi3 (PartitionInfo. "topic-b" 0 leader replicas isr)
          java-map (HashMap.)
          list-a (doto (ArrayList.) (.add pi1) (.add pi2))
          list-b (doto (ArrayList.) (.add pi3))
          _ (.put java-map "topic-a" list-a)
          _ (.put java-map "topic-b" list-b)
          result (topic-partitions-info->clj java-map)]
      (is (= 2 (count (get result "topic-a"))))
      (is (= 1 (count (get result "topic-b"))))
      (is (= "topic-a" (:topic (first (get result "topic-a")))))
      (is (= #{0 1} (set (map :partition (get result "topic-a"))))))))

(deftest test-roundtrip-conversions
  (testing "clj->topic-partition-offsets-map and topic-partition-offsets->clj are inverses"
    (let [original {{:topic "test" :partition 0} {:offset 50 :metadata "test-meta"}}
          java-map (clj->topic-partition-offsets-map original)
          roundtrip (topic-partition-offsets->clj java-map)]
      (is (= original roundtrip)))))