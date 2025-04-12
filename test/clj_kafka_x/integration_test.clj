(ns clj-kafka-x.integration-test
  (:require [clojure.test :refer :all]
            [clj-kafka-x.producer :as kp]
            [clj-kafka-x.consumers.simple :as kc])
  (:import [java.util UUID]))

(def ^:dynamic *run-integration-tests* false)
(def ^:dynamic *kafka-bootstrap-servers* "localhost:9092")
(def ^:dynamic *test-topic* "clj-kafka-x-test-topic")

(defn integration-fixture [f]
  (if *run-integration-tests*
    (try
      (println "Running integration tests with Kafka at" *kafka-bootstrap-servers*)
      (f)
      (catch Exception e
        (println "Error in integration tests:" (.getMessage e))
        (throw e)))
    (println "Skipping integration tests. Set *run-integration-tests* to true to run them.")))

(use-fixtures :once integration-fixture)

(deftest ^:integration test-produce-consume-cycle
  (testing "Full produce-consume cycle with real Kafka"
    (when *run-integration-tests*
      (let [test-id (str (UUID/randomUUID))
            producer-config {"bootstrap.servers" *kafka-bootstrap-servers*}
            consumer-config {"bootstrap.servers" *kafka-bootstrap-servers*
                             "group.id" (str "test-group-" test-id)
                             "auto.offset.reset" "earliest"}
            test-key "test-key"
            test-value (str "test-value-" test-id)]
        
        ;; Produce a message
        (with-open [producer (kp/producer producer-config 
                                         (kp/string-serializer) 
                                         (kp/string-serializer))]
          (let [record (kp/record *test-topic* test-key test-value)
                result @(kp/send producer record)]
            (is (= *test-topic* (:topic result)))
            (is (number? (:offset result)))))
        
        ;; Consume the message
        (with-open [consumer (kc/consumer consumer-config 
                                         (kc/string-deserializer) 
                                         (kc/string-deserializer))]
          (kc/subscribe consumer *test-topic*)
          
          ;; Poll for messages (with retry)
          (let [poll-with-timeout (fn []
                                    (let [msgs (kc/messages consumer :timeout 5000)]
                                      (if (seq msgs)
                                        msgs
                                        (do
                                          (Thread/sleep 1000)
                                          (kc/messages consumer :timeout 5000)))))
                messages (poll-with-timeout)]
            
            (is (seq messages) "Should have received at least one message")
            
            ;; Find our test message
            (let [test-message (first (filter #(= test-key (:key %)) messages))]
              (is test-message "Should have found the test message")
              (when test-message
                (is (= test-value (:value test-message)))
                (is (= *test-topic* (:topic test-message)))))))))))

(defn run-integration-tests []
  (binding [*run-integration-tests* true]
    (run-tests 'clj-kafka-x.integration-test)))