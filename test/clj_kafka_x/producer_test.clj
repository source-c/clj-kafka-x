(ns clj-kafka-x.producer-test
  (:require [clojure.test :refer :all]
            [clj-kafka-x.producer :as kp])
  (:import [org.apache.kafka.common.serialization StringSerializer ByteArraySerializer]
           [org.apache.kafka.clients.producer ProducerRecord MockProducer Partitioner]
           [org.apache.kafka.common TopicPartition Node PartitionInfo Cluster]
           [java.util Collections]))

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
      (is (= "test-value" (.value record)))))

  (testing "record handles nil key"
    (let [record (kp/record "test-topic" nil "test-value")]
      (is (nil? (.key record)))
      (is (= "test-value" (.value record)))))

  (testing "record handles byte array values"
    (let [value-bytes (.getBytes "test-value")
          record (kp/record "test-topic" value-bytes)]
      (is (= (seq value-bytes) (seq (.value record)))))))

;; Helper to create MockProducer for tests
(defn- round-robin-partitioner
  "Creates a simple round-robin partitioner for testing"
  []
  (let [counter (atom 0)]
    (reify Partitioner
      (partition [_ topic key key-bytes value value-bytes cluster]
        (let [partitions (.partitionsForTopic cluster topic)
              num-partitions (count partitions)]
          (if (pos? num-partitions)
            (mod (swap! counter inc) num-partitions)
            0)))
      (close [_])
      (configure [_ configs]))))

(defn- create-mock-producer
  "Creates a MockProducer with auto-complete enabled"
  []
  (MockProducer. true (round-robin-partitioner) (StringSerializer.) (StringSerializer.)))

(defn- create-mock-producer-with-cluster
  "Creates a MockProducer with a cluster containing partition info"
  []
  (let [node (Node. 0 "localhost" 9092)
        partition-info (PartitionInfo. "test-topic" 0 node
                                       (into-array Node [node])
                                       (into-array Node [node]))
        empty-set (java.util.Collections/emptySet)
        cluster (Cluster. "test-cluster"
                          (Collections/singletonList node)
                          (Collections/singletonList partition-info)
                          empty-set
                          empty-set)]
    (MockProducer. cluster true (round-robin-partitioner) (StringSerializer.) (StringSerializer.))))

(deftest test-send
  (testing "send returns a future that resolves to metadata map"
    (let [producer (create-mock-producer)
          record (kp/record "test-topic" "key" "value")
          future (kp/send producer record)
          result @future]
      (is (map? result))
      (is (= "test-topic" (:topic result)))
      (is (number? (:partition result)))
      (is (number? (:offset result)))
      (.close producer)))

  (testing "send with callback invokes callback on completion"
    (let [producer (create-mock-producer)
          record (kp/record "test-topic" "key" "value")
          callback-result (atom nil)
          callback-exception (atom :not-called)
          future (kp/send producer record
                          (fn [metadata exception]
                            (reset! callback-result metadata)
                            (reset! callback-exception exception)))]
      @future  ;; Wait for completion
      (Thread/sleep 50)  ;; Give callback time to execute
      (is (map? @callback-result))
      (is (= "test-topic" (:topic @callback-result)))
      (is (nil? @callback-exception))
      (.close producer)))

  (testing "send multiple records"
    (let [producer (create-mock-producer)
          records (map #(kp/record "test-topic" (str "key-" %) (str "value-" %))
                       (range 5))
          futures (mapv #(kp/send producer %) records)
          results (mapv deref futures)]
      (is (= 5 (count results)))
      (is (every? #(= "test-topic" (:topic %)) results))
      (is (= [0 1 2 3 4] (map :offset results)))
      (.close producer))))

(deftest test-flush
  (testing "flush does not throw"
    (let [producer (create-mock-producer)]
      (kp/send producer (kp/record "test-topic" "value"))
      (is (nil? (kp/flush producer)))
      (.close producer))))

(deftest test-close
  (testing "close without timeout"
    (let [producer (create-mock-producer)]
      (is (nil? (kp/close producer)))))

  (testing "close with timeout"
    (let [producer (create-mock-producer)]
      (is (nil? (kp/close producer 1000))))))

(deftest test-with-open
  (testing "producer works with with-open and is automatically closed"
    (let [producer-ref (atom nil)]
      (with-open [producer (create-mock-producer)]
        (reset! producer-ref producer)
        (kp/send producer (kp/record "test-topic" "key" "value"))
        (kp/flush producer)
        (is (= 1 (count (.history producer)))))
      ;; After with-open, producer should be closed
      (is (thrown? IllegalStateException
                   (kp/send @producer-ref (kp/record "test-topic" "another")))))))

(deftest test-producer-history
  (testing "MockProducer records sent messages"
    (let [producer (create-mock-producer)]
      (kp/send producer (kp/record "topic-a" "key1" "value1"))
      (kp/send producer (kp/record "topic-b" "key2" "value2"))
      (kp/flush producer)
      (let [history (.history producer)]
        (is (= 2 (count history)))
        (is (= "topic-a" (.topic (first history))))
        (is (= "value1" (.value (first history))))
        (is (= "topic-b" (.topic (second history))))
        (is (= "value2" (.value (second history)))))
      (.close producer))))

(deftest test-partitions
  (testing "partitions returns partition info"
    (let [producer (create-mock-producer-with-cluster)
          partitions (kp/partitions producer "test-topic")]
      (is (sequential? partitions))
      (is (= 1 (count partitions)))
      (is (= "test-topic" (:topic (first partitions))))
      (is (= 0 (:partition (first partitions))))
      (.close producer))))

(deftest test-metrics
  (testing "metrics returns a sequence of metric maps"
    (let [producer (create-mock-producer)
          metrics (kp/metrics producer)]
      (is (sequential? metrics))
      ;; MockProducer should have some metrics
      (when (seq metrics)
        (let [first-metric (first metrics)]
          (is (contains? first-metric :group))
          (is (contains? first-metric :name))
          (is (contains? first-metric :description))
          (is (contains? first-metric :tags))
          (is (contains? first-metric :value))))
      (.close producer))))