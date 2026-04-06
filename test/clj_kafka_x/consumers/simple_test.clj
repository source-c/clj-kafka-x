(ns clj-kafka-x.consumers.simple-test
  (:require [clojure.test :refer :all]
            [clj-kafka-x.consumers.simple :as kc])
  (:import [org.apache.kafka.common.serialization StringDeserializer ByteArrayDeserializer]
           [org.apache.kafka.clients.consumer MockConsumer OffsetResetStrategy ConsumerRecord]
           [org.apache.kafka.common TopicPartition]))

(deftest test-deserializers
  (testing "string-deserializer creates a StringDeserializer"
    (is (instance? StringDeserializer (kc/string-deserializer))))

  (testing "byte-array-deserializer creates a ByteArrayDeserializer"
    (is (instance? ByteArrayDeserializer (kc/byte-array-deserializer)))))

;; Helper functions for MockConsumer tests

(defn- create-mock-consumer
  "Creates a MockConsumer with EARLIEST offset reset strategy"
  []
  (MockConsumer. OffsetResetStrategy/EARLIEST))

(defn- setup-mock-consumer
  "Sets up a MockConsumer with topic-partition assignments and beginning offsets"
  [consumer topic partitions]
  (let [tps (map #(TopicPartition. topic %) partitions)
        offset-map (into {} (map #(vector % (long 0)) tps))]
    (.assign consumer tps)
    (.updateBeginningOffsets consumer offset-map)
    consumer))

(defn- add-record-to-consumer
  "Adds a ConsumerRecord to the MockConsumer"
  [consumer topic partition offset key value]
  (.addRecord consumer (ConsumerRecord. topic partition offset key value)))

;; Subscribe tests

(deftest test-subscribe-with-string
  (testing "subscribe with single topic string"
    (let [consumer (create-mock-consumer)]
      (kc/subscribe consumer "test-topic")
      (let [subs (.subscription consumer)]
        (is (= #{"test-topic"} (set subs))))
      (.close consumer))))

(deftest test-subscribe-with-sequence
  (testing "subscribe with sequence of topic strings"
    (let [consumer (create-mock-consumer)]
      (kc/subscribe consumer ["topic-a" "topic-b" "topic-c"])
      (let [subs (.subscription consumer)]
        (is (= #{"topic-a" "topic-b" "topic-c"} (set subs))))
      (.close consumer))))

(deftest test-subscribe-with-regex
  (testing "subscribe with regex pattern"
    (let [consumer (create-mock-consumer)]
      (kc/subscribe consumer #"topic-.*")
      ;; MockConsumer doesn't fully support pattern subscriptions,
      ;; but we can verify no exception is thrown
      (.close consumer))))

(deftest test-subscribe-with-manual-partition-assignment
  (testing "subscribe with explicit partition assignment"
    (let [consumer (create-mock-consumer)]
      (kc/subscribe consumer [{:topic "topic-a" :partitions #{0 1}}
                              {:topic "topic-b" :partitions #{0}}])
      (let [assignments (.assignment consumer)]
        (is (= 3 (count assignments)))
        (is (some #(and (= "topic-a" (.topic %)) (= 0 (.partition %))) assignments))
        (is (some #(and (= "topic-a" (.topic %)) (= 1 (.partition %))) assignments))
        (is (some #(and (= "topic-b" (.topic %)) (= 0 (.partition %))) assignments)))
      (.close consumer))))

(deftest test-subscribe-with-callbacks
  (testing "subscribe with assigned and revoked callbacks"
    (let [consumer (create-mock-consumer)
          assigned-partitions (atom nil)
          revoked-partitions (atom nil)]
      ;; Note: MockConsumer doesn't trigger rebalance callbacks,
      ;; so we're mainly testing that the function accepts the callbacks
      (kc/subscribe consumer "test-topic"
                    :assigned-callback #(reset! assigned-partitions %)
                    :revoked-callback #(reset! revoked-partitions %))
      (is (= #{"test-topic"} (set (.subscription consumer))))
      (.close consumer))))

(deftest test-subscribe-invalid-input
  (testing "subscribe throws on invalid input"
    (let [consumer (create-mock-consumer)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (kc/subscribe consumer 123)))
      (is (thrown? clojure.lang.ExceptionInfo
                   (kc/subscribe consumer ["topic-a" 1])))
      (is (thrown? clojure.lang.ExceptionInfo
                   (kc/subscribe consumer [{:topic "topic-a"}])))
      (is (thrown? clojure.lang.ExceptionInfo
                   (kc/subscribe consumer [{:topic "topic-a" :partitions ["0"]}])))
      (.close consumer))))

;; Subscriptions test

(deftest test-subscriptions
  (testing "subscriptions returns current subscriptions and assignments"
    (let [consumer (create-mock-consumer)]
      (kc/subscribe consumer [{:topic "topic-a" :partitions #{0 1}}
                              {:topic "topic-b" :partitions #{2}}])
      (let [subs (kc/subscriptions consumer)]
        (is (sequential? subs))
        (is (= 2 (count subs)))
        (let [topic-a-sub (first (filter #(= "topic-a" (:topic %)) subs))
              topic-b-sub (first (filter #(= "topic-b" (:topic %)) subs))]
          (is (= #{0 1} (:partitions topic-a-sub)))
          (is (= #{2} (:partitions topic-b-sub)))))
      (.close consumer))))

;; Unsubscribe test

(deftest test-unsubscribe
  (testing "unsubscribe removes all subscriptions"
    (let [consumer (create-mock-consumer)]
      (kc/subscribe consumer ["topic-a" "topic-b"])
      (is (= 2 (count (.subscription consumer))))
      (kc/unsubscribe consumer)
      (is (empty? (.subscription consumer)))
      (is (empty? (.assignment consumer)))
      (.close consumer))))

;; Seek tests

(deftest test-seek-to-offset
  (testing "seek to specific offset with topic and partition args"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0])
      (kc/seek consumer "test-topic" 0 100)
      (is (= 100 (.position consumer (TopicPartition. "test-topic" 0))))
      (.close consumer))))

(deftest test-seek-to-beginning
  (testing "seek to beginning"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0])
      ;; First seek to some offset
      (kc/seek consumer "test-topic" 0 50)
      (is (= 50 (.position consumer (TopicPartition. "test-topic" 0))))
      ;; Then seek to beginning
      (kc/seek consumer [{:topic "test-topic" :partition 0}] :beginning)
      (is (= 0 (.position consumer (TopicPartition. "test-topic" 0))))
      (.close consumer))))

(deftest test-seek-to-end
  (testing "seek to end"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0])
      (.updateEndOffsets consumer {(TopicPartition. "test-topic" 0) (long 100)})
      (kc/seek consumer [{:topic "test-topic" :partition 0}] :end)
      (is (= 100 (.position consumer (TopicPartition. "test-topic" 0))))
      (.close consumer))))

(deftest test-seek-multiple-partitions
  (testing "seek multiple partitions at once"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0 1 2])
      (kc/seek consumer [{:topic "test-topic" :partition 0}
                         {:topic "test-topic" :partition 1}
                         {:topic "test-topic" :partition 2}] 50)
      (is (= 50 (.position consumer (TopicPartition. "test-topic" 0))))
      (is (= 50 (.position consumer (TopicPartition. "test-topic" 1))))
      (is (= 50 (.position consumer (TopicPartition. "test-topic" 2))))
      (.close consumer))))

(deftest test-seek-invalid-offset
  (testing "seek throws on invalid offset"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0])
      (is (thrown? clojure.lang.ExceptionInfo
                   (kc/seek consumer [{:topic "test-topic" :partition 0}] :invalid)))
      (.close consumer))))

;; Messages tests

(deftest test-messages
  (testing "messages returns consumed records as sequence of maps"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0])
      (add-record-to-consumer consumer "test-topic" 0 0 "key1" "value1")
      (add-record-to-consumer consumer "test-topic" 0 1 "key2" "value2")
      (add-record-to-consumer consumer "test-topic" 0 2 "key3" "value3")
      (let [msgs (kc/messages consumer)]
        (is (= 3 (count msgs)))
        (is (= "key1" (:key (first msgs))))
        (is (= "value1" (:value (first msgs))))
        (is (= 0 (:offset (first msgs))))
        (is (= "test-topic" (:topic (first msgs))))
        (is (= 0 (:partition (first msgs)))))
      (.close consumer))))

(deftest test-messages-empty
  (testing "messages returns empty sequence when no records"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0])
      (let [msgs (kc/messages consumer :timeout 100)]
        (is (empty? msgs)))
      (.close consumer))))

(deftest test-messages-with-custom-timeout
  (testing "messages accepts custom timeout"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0])
      (let [msgs (kc/messages consumer :timeout 500)]
        (is (sequential? msgs)))
      (.close consumer))))

;; Commit tests

(deftest test-commit-sync
  (testing "commit-sync without arguments"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0])
      (add-record-to-consumer consumer "test-topic" 0 0 "key" "value")
      (kc/messages consumer)
      ;; commit-sync should not throw
      (is (nil? (kc/commit-sync consumer)))
      (.close consumer))))

(deftest test-commit-sync-with-offsets
  (testing "commit-sync with specific offsets"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0])
      (let [offsets {{:topic "test-topic" :partition 0} {:offset 10 :metadata "test"}}]
        (is (nil? (kc/commit-sync consumer offsets))))
      (.close consumer))))

(deftest test-commit-async
  (testing "commit-async without arguments"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0])
      (add-record-to-consumer consumer "test-topic" 0 0 "key" "value")
      (kc/messages consumer)
      (is (nil? (kc/commit-async consumer)))
      (.close consumer))))

(deftest test-commit-async-with-callback
  (testing "commit-async with callback"
    (let [consumer (create-mock-consumer)
          callback-called (atom false)]
      (setup-mock-consumer consumer "test-topic" [0])
      (add-record-to-consumer consumer "test-topic" 0 0 "key" "value")
      (kc/messages consumer)
      (kc/commit-async consumer (fn [offsets exception]
                                  (reset! callback-called true)))
      ;; Note: MockConsumer may not trigger the callback
      (.close consumer))))

;; Pause and Resume tests

(deftest test-pause
  (testing "pause stops consumption from specified partitions"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0 1])
      (kc/pause consumer [{:topic "test-topic" :partition 0}])
      (let [paused (.paused consumer)]
        (is (= 1 (count paused)))
        (is (= "test-topic" (.topic (first paused))))
        (is (= 0 (.partition (first paused)))))
      (.close consumer))))

(deftest test-resume
  (testing "resume restarts consumption from paused partitions"
    (let [consumer (create-mock-consumer)]
      (setup-mock-consumer consumer "test-topic" [0 1])
      (kc/pause consumer [{:topic "test-topic" :partition 0}
                          {:topic "test-topic" :partition 1}])
      (is (= 2 (count (.paused consumer))))
      (kc/resume consumer [{:topic "test-topic" :partition 0}])
      (let [paused (.paused consumer)]
        (is (= 1 (count paused)))
        (is (= 1 (.partition (first paused)))))
      (.close consumer))))

;; Metrics test

(deftest test-consumer-metrics
  (testing "metrics returns a sequence of metric maps"
    (let [consumer (create-mock-consumer)]
      (let [metrics (kc/metrics consumer)]
        (is (sequential? metrics))
        ;; MockConsumer should have some metrics
        (when (seq metrics)
          (let [first-metric (first metrics)]
            (is (contains? first-metric :group))
            (is (contains? first-metric :name))
            (is (contains? first-metric :description))
            (is (contains? first-metric :tags))
            (is (contains? first-metric :value)))))
      (.close consumer))))

;; with-open test

(deftest test-with-open
  (testing "consumer works with with-open and is automatically closed"
    (let [consumer-ref (atom nil)]
      (with-open [consumer (create-mock-consumer)]
        (reset! consumer-ref consumer)
        (setup-mock-consumer consumer "test-topic" [0])
        (add-record-to-consumer consumer "test-topic" 0 0 "key" "value")
        (let [msgs (kc/messages consumer)]
          (is (= 1 (count msgs)))))
      ;; After with-open, consumer should be closed
      (is (thrown? IllegalStateException
                   (kc/messages @consumer-ref))))))
