(ns ^{:doc "Clojure interface for Kafka Share Consumer API (KIP-932).
            Enables multiple consumers to read from the same partition,
            solving the Head of Line Blocking problem.
            For complete JavaDocs, see:
            https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaShareConsumer.html"}
  clj-kafka-x.consumers.shared
  (:require [clj-kafka-x.data :refer [to-clojure]]
            [clj-kafka-x.impl.helpers :refer [coerce-config client-metrics]])
  (:import [org.apache.kafka.clients.consumer ShareConsumer KafkaShareConsumer
            AcknowledgeType AcknowledgementCommitCallback ConsumerConfig]
           [org.apache.kafka.common.serialization ByteArrayDeserializer Deserializer StringDeserializer]
           (java.util Map Collection)
           (java.time Duration)))

(def ^:private config-def (ConsumerConfig/configDef))

(defn string-deserializer [] (StringDeserializer.))
(defn byte-array-deserializer [] (ByteArrayDeserializer.))

(def ^:private acknowledge-types
  {:accept  AcknowledgeType/ACCEPT
   :release AcknowledgeType/RELEASE
   :reject  AcknowledgeType/REJECT
   :renew   AcknowledgeType/RENEW})

(defn share-consumer
  "Takes a map of config options and returns a `KafkaShareConsumer` for consuming
  records from Kafka using share groups. Multiple consumers in the same group can
  read from the same partition concurrently.

  NOTE `KafkaShareConsumer` instances are NOT thread-safe.

  For more information and available config options,
  see: https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaShareConsumer.html

  Usage:

  (share-consumer {\"bootstrap.servers\" \"localhost:9092\"
                   \"group.id\" \"my-share-group\"
                   \"key.deserializer\" \"org.apache.kafka.common.serialization.StringDeserializer\"
                   \"value.deserializer\" \"org.apache.kafka.common.serialization.StringDeserializer\"})

  (share-consumer {\"bootstrap.servers\" \"localhost:9092\"
                   \"group.id\" \"my-share-group\"} (string-deserializer) (string-deserializer))

  (with-open [c (share-consumer config (string-deserializer) (string-deserializer))]
    (subscribe c \"topic-a\")
    (take 5 (messages c)))
  "
  ([^Map config]
   (KafkaShareConsumer. ^Map (coerce-config config-def config)))
  ([^Map config ^Deserializer key-deserializer ^Deserializer value-deserializer]
   (KafkaShareConsumer. ^Map (coerce-config config-def config) key-deserializer value-deserializer)))


(defn subscribe
  "Subscribes the share consumer to the given topic(s).
  Takes a single topic name or a sequence of topic names.

  NOTE Share consumers only support topic-name subscriptions.
  Pattern and manual partition assignment are not supported.

  Usage:

  (subscribe consumer \"topic-a\")
  ;; => nil

  (subscribe consumer [\"topic-a\" \"topic-b\"])
  ;; => nil
  "
  [^ShareConsumer consumer topics]
  (let [topics ^Collection (cond
                             (string? topics) (vector topics)
                             (sequential? topics) topics
                             :else (throw
                                     (ex-info "Topic should be a string or sequence of strings"
                                              {:topic topics})))]
    (.subscribe consumer topics)))


(defn subscriptions
  "Returns the set of topics the share consumer is subscribed to.

  Usage:

  (subscriptions consumer)
  ;; => #{\"topic-a\" \"topic-b\"}
  "
  [^ShareConsumer consumer]
  (into #{} (.subscription consumer)))


(defn unsubscribe
  "Unsubscribes the share consumer from all subscribed topics."
  [^ShareConsumer consumer]
  (.unsubscribe consumer))


(defn messages
  "Consumes messages from currently subscribed partitions and returns a sequence of messages.
  If no messages are available, it will use the provided timeout (or default of 1000ms)
  to BLOCK for messages to be available, before returning.

  Usage:

  (messages consumer)
  ;; => [{:topic \"topic-a\", :partition 0, :offset 0, :key nil, :value \"hello\"}]

  (messages consumer :timeout 1500)
  ;; => []
  "
  [^ShareConsumer consumer & {:keys [timeout] :or {timeout 1000}}]
  (let [duration (Duration/ofMillis timeout)
        consumer-records (.poll consumer duration)]
    (to-clojure consumer-records)))


(defn acknowledge
  "Acknowledges a consumed record. In explicit acknowledgement mode, every record
  must be acknowledged before the next poll.

  The acknowledgement type can be:
    :accept  - record consumed successfully (default)
    :release - not consumed, release for redelivery
    :reject  - not consumed, reject permanently
    :renew   - still processing, renew acquisition lock

  Usage:

  (acknowledge consumer record)
  ;; => nil

  (acknowledge consumer record :reject)
  ;; => nil
  "
  ([^ShareConsumer consumer record]
   (.acknowledge consumer record))
  ([^ShareConsumer consumer record ack-type]
   (let [type (or (get acknowledge-types ack-type)
                  (throw (ex-info "Unknown acknowledge type, expected one of :accept :release :reject :renew"
                                  {:ack-type ack-type})))]
     (.acknowledge consumer record type))))


(defn- commit-result->clj [^Map result]
  (reduce (fn [m [tip opt-ex]]
            (assoc m (to-clojure tip) (when (.isPresent opt-ex) (.get opt-ex))))
          {} result))

(defn commit-sync
  "Commits the acknowledgements for the share consumer synchronously.
  Returns a map of {topic-partition -> exception-or-nil}.

  Usage:

  (commit-sync consumer)
  ;; => {{:topic \"topic-a\", :partition 0} nil}
  "
  ([^ShareConsumer consumer]
   (commit-result->clj (.commitSync consumer)))
  ([^ShareConsumer consumer timeout-ms]
   (commit-result->clj (.commitSync consumer (Duration/ofMillis timeout-ms)))))


(defn commit-async
  "Commits the acknowledgements for the share consumer asynchronously.

  Usage:

  (commit-async consumer)
  ;; => nil

  (commit-async consumer (fn [offsets exception]
                           (if exception
                             (println \"Commit failed:\" exception)
                             (println \"Committed:\" offsets))))
  ;; => nil
  "
  ([^ShareConsumer consumer]
   (.commitAsync consumer))
  ([^ShareConsumer consumer callback-fn]
   (let [callback (reify AcknowledgementCommitCallback
                    (onComplete [_ offsets exception]
                      (callback-fn (reduce (fn [m [tip offset-set]]
                                             (assoc m (to-clojure tip) (into #{} offset-set)))
                                           {} offsets)
                                   exception)))]
     (.setAcknowledgementCommitCallback consumer callback)
     (.commitAsync consumer))))


(defn close
  "Closes the share consumer.

  See: https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaShareConsumer.html#close()
  "
  ([^ShareConsumer consumer]
   (.close consumer))
  ([^ShareConsumer consumer timeout-ms]
   (.close consumer (Duration/ofMillis timeout-ms))))


(defn metrics
  "Returns a sequence of maps representing all the share consumer's internal metrics.
   Each map contains :group, :name, :description, :tags and :value.

  Usage:

  (metrics consumer)
  ;; => [{:group \"consumer-share-metrics\", :name \"fetch-rate\", ...}]
  "
  [^ShareConsumer consumer]
  (client-metrics consumer))
