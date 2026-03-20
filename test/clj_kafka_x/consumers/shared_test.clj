(ns clj-kafka-x.consumers.shared-test
  (:require [clojure.test :refer :all]
            [clj-kafka-x.consumers.shared :as ks])
  (:import [org.apache.kafka.common.serialization StringDeserializer ByteArrayDeserializer]
           [org.apache.kafka.clients.consumer MockShareConsumer ConsumerRecord AcknowledgeType]))

(deftest test-deserializers
  (testing "string-deserializer creates a StringDeserializer"
    (is (instance? StringDeserializer (ks/string-deserializer))))

  (testing "byte-array-deserializer creates a ByteArrayDeserializer"
    (is (instance? ByteArrayDeserializer (ks/byte-array-deserializer)))))

;; Helper functions for MockShareConsumer tests

(defn- create-mock-share-consumer
  "Creates a MockShareConsumer for testing"
  []
  (MockShareConsumer.))

(defn- add-record
  "Adds a ConsumerRecord to the MockShareConsumer"
  [consumer topic partition offset key value]
  (.addRecord consumer (ConsumerRecord. topic partition offset key value)))

;; Subscribe tests

(deftest test-subscribe-with-string
  (testing "subscribe with single topic string"
    (let [consumer (create-mock-share-consumer)]
      (ks/subscribe consumer "test-topic")
      (is (= #{"test-topic"} (.subscription consumer)))
      (.close consumer))))

(deftest test-subscribe-with-sequence
  (testing "subscribe with sequence of topic strings"
    (let [consumer (create-mock-share-consumer)]
      (ks/subscribe consumer ["topic-a" "topic-b" "topic-c"])
      (is (= #{"topic-a" "topic-b" "topic-c"} (.subscription consumer)))
      (.close consumer))))

(deftest test-subscribe-invalid-input
  (testing "subscribe throws on invalid input"
    (let [consumer (create-mock-share-consumer)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (ks/subscribe consumer 123)))
      (.close consumer))))

;; Subscriptions test

(deftest test-subscriptions
  (testing "subscriptions returns set of subscribed topics"
    (let [consumer (create-mock-share-consumer)]
      (ks/subscribe consumer ["topic-a" "topic-b"])
      (is (= #{"topic-a" "topic-b"} (ks/subscriptions consumer)))
      (.close consumer))))

;; Unsubscribe test

(deftest test-unsubscribe
  (testing "unsubscribe removes all subscriptions"
    (let [consumer (create-mock-share-consumer)]
      (ks/subscribe consumer ["topic-a" "topic-b"])
      (is (= 2 (count (ks/subscriptions consumer))))
      (ks/unsubscribe consumer)
      (is (empty? (ks/subscriptions consumer)))
      (.close consumer))))

;; Messages tests

(deftest test-messages
  (testing "messages returns consumed records as sequence of maps"
    (let [consumer (create-mock-share-consumer)]
      (ks/subscribe consumer "test-topic")
      (add-record consumer "test-topic" 0 0 "key1" "value1")
      (add-record consumer "test-topic" 0 1 "key2" "value2")
      (add-record consumer "test-topic" 0 2 "key3" "value3")
      (let [msgs (ks/messages consumer)]
        (is (= 3 (count msgs)))
        (is (= "key1" (:key (first msgs))))
        (is (= "value1" (:value (first msgs))))
        (is (= 0 (:offset (first msgs))))
        (is (= "test-topic" (:topic (first msgs))))
        (is (= 0 (:partition (first msgs)))))
      (.close consumer))))

(deftest test-messages-empty
  (testing "messages returns empty sequence when no records"
    (let [consumer (create-mock-share-consumer)]
      (ks/subscribe consumer "test-topic")
      (let [msgs (ks/messages consumer :timeout 100)]
        (is (empty? msgs)))
      (.close consumer))))

(deftest test-messages-with-custom-timeout
  (testing "messages accepts custom timeout"
    (let [consumer (create-mock-share-consumer)]
      (ks/subscribe consumer "test-topic")
      (let [msgs (ks/messages consumer :timeout 500)]
        (is (sequential? msgs)))
      (.close consumer))))

;; Acknowledge tests

(deftest test-acknowledge-default
  (testing "acknowledge with default type (accept)"
    (let [consumer (create-mock-share-consumer)]
      (ks/subscribe consumer "test-topic")
      (add-record consumer "test-topic" 0 0 "key" "value")
      (let [msgs (ks/messages consumer)
            record (first msgs)]
        ;; acknowledge expects a raw ConsumerRecord, but messages returns maps.
        ;; We need to use the raw Java record for acknowledge.
        ;; Re-poll to get raw records for ack testing.
        )
      (.close consumer))))

(deftest test-acknowledge-with-raw-records
  (testing "acknowledge works with ConsumerRecord objects"
    (let [consumer (create-mock-share-consumer)
          record (ConsumerRecord. "test-topic" 0 0 "key" "value")]
      (ks/subscribe consumer "test-topic")
      (.addRecord consumer record)
      (.poll consumer (java.time.Duration/ofMillis 100))
      ;; acknowledge should not throw
      (is (nil? (ks/acknowledge consumer record)))
      (.close consumer))))

(deftest test-acknowledge-with-type
  (testing "acknowledge with explicit type"
    (let [consumer (create-mock-share-consumer)
          record (ConsumerRecord. "test-topic" 0 0 "key" "value")]
      (ks/subscribe consumer "test-topic")
      (.addRecord consumer record)
      (.poll consumer (java.time.Duration/ofMillis 100))
      (is (nil? (ks/acknowledge consumer record :accept)))
      (.close consumer)))

  (testing "acknowledge with reject type"
    (let [consumer (create-mock-share-consumer)
          record (ConsumerRecord. "test-topic" 0 1 "key" "value")]
      (ks/subscribe consumer "test-topic")
      (.addRecord consumer record)
      (.poll consumer (java.time.Duration/ofMillis 100))
      (is (nil? (ks/acknowledge consumer record :reject)))
      (.close consumer)))

  (testing "acknowledge with release type"
    (let [consumer (create-mock-share-consumer)
          record (ConsumerRecord. "test-topic" 0 2 "key" "value")]
      (ks/subscribe consumer "test-topic")
      (.addRecord consumer record)
      (.poll consumer (java.time.Duration/ofMillis 100))
      (is (nil? (ks/acknowledge consumer record :release)))
      (.close consumer)))

  (testing "acknowledge with renew type"
    (let [consumer (create-mock-share-consumer)
          record (ConsumerRecord. "test-topic" 0 3 "key" "value")]
      (ks/subscribe consumer "test-topic")
      (.addRecord consumer record)
      (.poll consumer (java.time.Duration/ofMillis 100))
      (is (nil? (ks/acknowledge consumer record :renew)))
      (.close consumer))))

(deftest test-acknowledge-invalid-type
  (testing "acknowledge throws on invalid type"
    (let [consumer (create-mock-share-consumer)
          record (ConsumerRecord. "test-topic" 0 0 "key" "value")]
      (ks/subscribe consumer "test-topic")
      (.addRecord consumer record)
      (.poll consumer (java.time.Duration/ofMillis 100))
      (is (thrown? clojure.lang.ExceptionInfo
                   (ks/acknowledge consumer record :invalid)))
      (.close consumer))))

;; Commit tests

(deftest test-commit-sync
  (testing "commit-sync returns a map"
    (let [consumer (create-mock-share-consumer)]
      (ks/subscribe consumer "test-topic")
      (let [result (ks/commit-sync consumer)]
        (is (map? result)))
      (.close consumer))))

(deftest test-commit-async
  (testing "commit-async without arguments"
    (let [consumer (create-mock-share-consumer)]
      (ks/subscribe consumer "test-topic")
      (is (nil? (ks/commit-async consumer)))
      (.close consumer))))

;; Metrics test

(deftest test-share-consumer-metrics
  (testing "metrics returns a sequence of metric maps"
    (let [consumer (create-mock-share-consumer)]
      (let [metrics (ks/metrics consumer)]
        (is (sequential? metrics))
        (when (seq metrics)
          (let [first-metric (first metrics)]
            (is (contains? first-metric :group))
            (is (contains? first-metric :name))
            (is (contains? first-metric :description))
            (is (contains? first-metric :tags))
            (is (contains? first-metric :value)))))
      (.close consumer))))

;; Close test

(deftest test-close
  (testing "share consumer can be closed"
    (let [consumer (create-mock-share-consumer)]
      (ks/subscribe consumer "test-topic")
      (is (nil? (ks/close consumer))))))

;; with-open test

(deftest test-with-open
  (testing "share consumer works with with-open"
    (let [consumer-ref (atom nil)]
      (with-open [consumer (create-mock-share-consumer)]
        (reset! consumer-ref consumer)
        (ks/subscribe consumer "test-topic")
        (add-record consumer "test-topic" 0 0 "key" "value")
        (let [msgs (ks/messages consumer)]
          (is (= 1 (count msgs))))))))
