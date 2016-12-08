(ns ^{:doc "Clojure interface for Kafka Consumer API. For
  complete JavaDocs, see:
  http://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/package-summary.html"}
    clj-kafka-x.consumer
  (:require [clj-kafka-x.data :refer :all])
  (:import java.util.List
           java.util.regex.Pattern
           [org.apache.kafka.clients.consumer ConsumerRebalanceListener KafkaConsumer OffsetAndMetadata OffsetCommitCallback]
           [org.apache.kafka.common.serialization ByteArrayDeserializer Deserializer StringDeserializer]
           org.apache.kafka.common.TopicPartition))


(defn string-deserializer [] (StringDeserializer.))
(defn byte-array-deserializer [] (ByteArrayDeserializer.))

(defn consumer
  "Takes a map of config options and returns a `KafkaConsumer` for consuming records from Kafka.

  NOTE `KafkaConsumer` instances are NOT thread-safe, see https://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#multithreaded

  For more information and available conifg options,
  see: http://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
       http://kafka.apache.org/documentation.html#newconsumerconfigs

  Usage:

;; Created using just a map of configs, in this case the keys
;; bootstrap.servers value.deserializer and key.deserializer are required
 (consumer {\"bootstrap.servers\" \"localhost:9092\"
            \"group.id\" \"test-group-id\"
            \"value.deserializer\" \"org.apache.kafka.common.serialization.StringDeserializer\"
            \"key.deserializer\" \"org.apache.kafka.common.serialization.StringDeserializer\"})

;; Created using a map of configs and the deserializers for keys and values.
 (consumer {\"bootstrap.servers\" \"localhost:9092\"
            \"group.id\" \"test-group-id\"} (string-deserializer) (string-deserializer))

;; KafkaConsumer should be closed when not used anymore, as it's closeable,
;; it can be used in the with-open macro
  (def config {\"bootstrap.servers\" \"localhost:9092\"
               \"group.id\" \"test-group-id\"})
  (with-open [c (consumer config (string-deserializer) (string-deserializer))]
    (subscribe c \"topic-a\")
    (take 5 (messages c)))

  "
  ([^java.util.Map config]
   (KafkaConsumer. config))
  ([^java.util.Map config ^Deserializer key-deserializer ^Deserializer value-deserializer]
   (KafkaConsumer. config key-deserializer value-deserializer)))


(defn subscribe
  "Subscribes the consumer to Topic partition(s) with callbacks for broker initiated assignments.
  The actual partitions can actually be specified (manual assignment) or left up to the Kafka broker (automatic assignment).
  This function performs 3 forms of subscription and they are -
  1) Single or Sequence of topic names             <---- Automatic partition assignment by Kafka Broker
  2) Regular expression matching topic name(s)     <---- Automatic partition assignment by Kafka Broker
  3) A sequence of specific topic partitions       <---- Manual partition assignment by user/client/consumer

  NOTE a)The above 3 forms are mutually exclusive, meaning you need to unsubcsribe in between subscribing using different forms
       b)Calling subscribe again with the same form but different arguments is equivalent to unsubscribing and then subscribing anew.
       c)The optional callback function arguments are only used for Automatic partition subscriptions
         i.e subcriptions using single name, sequence of names or regular expression
         The callback functions should be of a single arity and should expect a sequence of maps describing
         specific partitions (e.g [{:topic \"topic-a\" :partition 1} {:topic \"topic-a\" :partition 2}])

  Usage:

  (subscribe consumer \"topic-a\")
  ;; => nil

  (subscribe consumer \"topic-a\" :assigned-callback (fn [p] (println \"PartitionsAssigned:\" p))
                                      :revoked-callback (fn [p] (println \"PartitionsRevoked:\" p)))
  ;; => nil

  (subscribe consumer [\"topic-a\" \"topic-b\"])
  ;; => nil

  (subscribe consumer #\"topic-.+\")
  ;; => nil

  (subscribe consumer [{:topic \"topic-a\" :partitions #{0}}
                       {:topic \"topic-b\" :partitions #{0 1}}
                       {:topic \"topic-c\" :partitions #{0}}])
  ;; => nil

  For more in-depth information
  http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#subscribe(java.util.List)
  http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#subscribe(java.util.List,%20org.apache.kafka.clients.consumer.ConsumerRebalanceListener)
  http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#subscribe(java.util.regex.Pattern,%20org.apache.kafka.clients.consumer.ConsumerRebalanceListener)
  http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#assign(java.util.List)
  "
  [^KafkaConsumer consumer topics & {:keys [assigned-callback revoked-callback]
                                     :or {assigned-callback (fn [_])
                                          revoked-callback (fn [_])}}]
  ;;TODO needs to be cleaned up and refactored
  (let [listener (reify ConsumerRebalanceListener
                   (onPartitionsAssigned [_ partitions] (assigned-callback (mapv to-clojure partitions)))
                   (onPartitionsRevoked [_ partitions] (revoked-callback (mapv to-clojure partitions))))
        topics (cond
                 (string? topics) (vector topics)
                 (and (sequential? topics) (string? (first topics))) topics
                 (= Pattern (type topics)) topics
                 (and (sequential? topics) (map? (first topics))) topics
                 :else (throw
                        (ex-info "Topic should be a string, sequence (of strings or maps) or pattern"
                                 {:topic topics})))]

    (if (and (sequential? topics) (map? (first topics)))
      (do
        (let [expand-tps (fn [{:keys [topic partitions]}]
                           (reduce #(conj %1 (map->topic-partition {:topic topic :partition %2})) [] partitions))
              exploded-tps (mapcat expand-tps topics)]
          (.assign consumer exploded-tps)))
      (.subscribe consumer topics listener))))


(defn subscriptions
  "Returns all the topics that the consumer is subscribed to and the actual partitions
  that it's consuming from. The data is a sequence of maps with each map being made up
  of topic (assoc with :topic) and a set of consumed partitions (assoc with :partitions)
  NOTE Subscriptions made using only topics (names or regex patterns) will have their
       partitions automatically assigned/managed by the broker. This can lead a consumer
       to be subscribed to a topic but NOT consuming from any of it's partitions.
       (see the topic-c in the Usage example below)

  Usage:

  (subscriptions consumer)
  ;; => [{:topic \"topic-a\", :partitions #{0}},
  ;;     {:topic \"topic-b\", :partitions #{0 1 2}},
  ;;     {:topic \"topic-c\", :partitions #{}}]
  "
  [^KafkaConsumer consumer]
  ;;TODO is this clear and readable enough ? refactor?
  (let [auto-subs (.subscription consumer)
        manual-subs (.assignment consumer)
        subs (reduce #(assoc %1 %2 {:topic %2 :partitions #{}})  {} auto-subs)
        reduce-fn (fn [m tp-object]
                    (let [tp (to-clojure tp-object)
                          t (:topic tp)
                          p (:partition tp)]
                      (update m t #(if %1
                                     (update %1 :partitions conj p)
                                     {:topic t :partitions #{p}}))))]
    (->> (reduce reduce-fn subs manual-subs)
        (mapv val))))


(defn unsubscribe
  "Unsubcribes the consumer from any subscribed topics and/or partitions.
   It works for subscriptions carried out via subscribe-to-topics or subscribe-to-partitions functions"
  [^KafkaConsumer consumer]
  (.unsubscribe consumer))

(defn seek
  "Seeks the consumer offset to given offset on the topic-partitions.

   NOTE The topic-partition can be given as 2 arguments, the topic (string) and partition (int)
   or it can be given as 1 argument, which is a map sequence e.g '({:topic \"topic\" :partition 2}).
   The offset can be a long, :beginning or :end.

  Usage:

  (seek consumer \"topic-a\" 23 7)
  ;; => nil

  (seek consumer \"topic-b\" 23 :beginning)
  ;; => nil

  (seek consumer \"topic-c\" 23 :end)
  ;; => nil

  (seek consumer [{:topic \"topic-a\" :partition 23}
                  {:topic \"topic-b\" :partition 23}
                  {:topic \"topic-c\" :partition 23}] 7)
  ;; => nil

  (seek consumer [{:topic \"topic-a\" :partition 23}
                  {:topic \"topic-b\" :partition 23}
                  {:topic \"topic-c\" :partition 23}] :beginning)
  ;; => nil

  (seek consumer [{:topic \"topic-a\" :partition 23}
                  {:topic \"topic-b\" :partition 23}
                  {:topic \"topic-c\" :partition 23}] :end)
  ;; => nil

  "
  ([^KafkaConsumer consumer topic partition offset]
   (seek consumer (vector {:topic topic :partition partition}) offset))
  ([^KafkaConsumer consumer tp-seq offset]
   (let [tp-class-seq (map map->topic-partition tp-seq)
         tp-class-array (into-array TopicPartition tp-class-seq)]
     (cond
       (= :beginning offset) (.seekToBeginning consumer tp-class-array)
       (= :end offset) (.seekToEnd consumer tp-class-array)
       (integer? offset) (run! #(.seek consumer % offset) tp-class-seq)
       :else (throw (ex-info "offset should be :beginning :end or a number"
                             {:offset offset}))))))

(defn messages
  "Consumes messages from currently subscribed partitions and returns a sequence of messages.
  If no messages are available, it will use the provided timeout (or default of 1000ms)
  to BLOCK for messages to be available, before returning.

  Usage:

  (messages consumer)
  ;; => [{:topic \"topic-a\",
  ;;      :partition 0,
  ;;      :offset 0,
  ;;      :key nil,
  ;;      :value \"Count Zero says 1 at Fri Mar 11 14:34:27 GMT 2016\"}
  ;;     {:topic \"topic-a\",
  ;;      :partition 0,
  ;;      :offset 1,
  ;;      :key nil,
  ;;      :value \"Count Zero says 2 at Fri Mar 11 14:34:31 GMT 2016\"}]

  (messages consumer :timeout 1500)
  ;; => [{:topic \"topic-a\",
  ;;      :partition 0,
  ;;      :offset 2,
  ;;      :key nil,
  ;;      :value \"Count Zero says 3 at Fri Mar 11 14:34:32 GMT 2016\"}]

  "
  [^KafkaConsumer consumer & {:keys [timeout] :or {timeout 1000}}]

  (let [consumer-records (.poll consumer timeout)]
    (to-clojure consumer-records)))



(defn commit-async
  "Commits the offsets of messages returned by the last call to the messages function or the given offsets.

  NOTE This is done aysnchronously and will return immediately.
  (Based on the code in kafka-clients 0.9.0.0 the commit request is not
   actually made until the next time the messages function is called)

  Usage:

  ; Commits all the offsets received from the last call to the messages function.
  ; Exceptions/Errors are ignored
  (commit-async consumer)
  ;; => nil


  ; Commits all the offsets received from the last call to the messages function.
  ; Success or failure is handled by the given callback function
  (commit-async consumer (fn [offsets exception]
                          (if exception
                             (println \"Commits failed for \" offsets \" Exception->\" exception)
                             (println \"Commits passed for \" offsets))))
  ;; => nil


  ; Commits the specified offsets to the specific topic-partitions.
  ; Success or failure is handled by the given callback function
  (def tp-om   {{:topic \"topic-a\", :partition 4} {:offset 24, :metadata \"important commit\"},
                {:topic \"topic-a\", :partition 1} {:offset 234, :metadata \"commited by thread A\"},
                {:topic \"topic-b\", :partition 7} {:offset 23, :metadata \"commited on 12/12/12\"}})

  (commit-async consumer tp-om (fn [offsets exception]
                                (if exception
                                   (println \"Commits failed for \" offsets \" Exception->\" exception)
                                   (println \"Commits passed for \" offsets))))
  ;; => nil
  "
  ([^KafkaConsumer consumer] (.commitAsync consumer))
  ([^KafkaConsumer consumer offset-commit-fn]
   (let [callback (reify OffsetCommitCallback
                    (onComplete [_ offsets exception]
                      (offset-commit-fn (tp-om-map->map offsets) exception)))]
     (.commitAsync consumer callback)))
  ([^KafkaConsumer consumer topic-partition-offsets-metadata offset-commit-fn]
   (let [callback (reify OffsetCommitCallback
                    (onComplete [_ offsets exception]
                      (offset-commit-fn (tp-om-map->map offsets) exception)))
         tp-om-map (map->tp-om-map topic-partition-offsets-metadata)]
     (.commitAsync consumer tp-om-map callback))))


(defn commit-sync
  "Commits the offsets of messages returned by the last call to the messages function or the given offsets.
  NOTE This is a blocking I/O operation and will throw an Exception on failure

  Usage:

  ; Commits all the offsets received from the last call to the messages function.
  ; If there's any failure, an Exception is thrown.
  (commit-sync consumer)
  ;; => nil

  ; Commits the specified offsets to the specific topic-partitions.
  ; If there's any failure, an Exception is thrown.
  (def tp-om   {{:topic \"topic-a\", :partition 4} {:offset 24, :metadata \"important commit\"},
                {:topic \"topic-a\", :partition 1} {:offset 234, :metadata \"commited by thread A\"},
                {:topic \"topic-b\", :partition 7} {:offset 23, :metadata \"commited on 12/12/12\"}})

  (commit-sync consumer tp-om)
  ;; => nil
  "
  ([^KafkaConsumer consumer] (.commitSync consumer))
  ([^KafkaConsumer consumer topic-partitions-offsets-metadata]
   (let [tp-om-map (map->tp-om-map topic-partitions-offsets-metadata)]
     (.commitSync consumer tp-om-map))))


(defn last-committed-offset
  "Gets the last committed offset for the partition of a topic.
   NOTE This function is a blocking I/O operation.

   see http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#committed(org.apache.kafka.common.TopicPartition)

  Usage:

  (last-committed-offset consumer {:topic \"topic-a\" :partition 2})
  ;; => {:offset 10, :metadata \"Metadata set during commit\"}
  "
  [^KafkaConsumer consumer tp]
  (->> tp
       map->topic-partition
       (.committed consumer)
       to-clojure))


(defn list-all-topics
  "Get metadata about ALL partitions for ALL topics that the user is authorized to view.
   NOTE This function is a blocking I/O operation.

   See http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#listTopics()

  Usage :

  (list-all-topics consumer)
  ;; =>{\"topic-a\"
  ;;    [{:topic \"topic-a\",
  ;;      :partition 0,
  ;;      :leader {:id 3, :host \"172.17.0.5\", :port 9094},
  ;;      :replicas [{:id 3, :host \"172.17.0.5\", :port 9094}],
  ;;      :in-sync-replicas [{:id 3, :host \"172.17.0.5\", :port 9094}]}],
  ;;    \"topic-b\"
  ;;    [{:topic \"topic-b\",
  ;;      :partition 2,
  ;;      :leader {:id 1, :host \"172.17.0.4\", :port 9092},
  ;;      :replicas [{:id 1, :host \"172.17.0.4\", :port 9092}],
  ;;      :in-sync-replicas [{:id 1, :host \"172.17.0.4\", :port 9092}]}
  ;;      {:topic \"topic-b\",
  ;;      :partition 1,
  ;;      :leader {:id 3, :host \"172.17.0.5\", :port 9094},
  ;;      :replicas [{:id 3, :host \"172.17.0.5\", :port 9094}],
  ;;      :in-sync-replicas [{:id 3, :host \"172.17.0.5\", :port 9094}]}
  ;;      {:topic \"topic-b\",
  ;;      :partition 0,
  ;;      :leader {:id 2, :host \"172.17.0.3\", :port 9093},
  ;;      :replicas [{:id 2, :host \"172.17.0.3\", :port 9093}],
  ;;      :in-sync-replicas [{:id 2, :host \"172.17.0.3\", :port 9093}]}]}
  "
  [^KafkaConsumer consumer]
  (str-pi-map->map (.listTopics consumer)))

(defn list-all-partitions
  "Get metadata about all partitions for a particular topic.
   NOTE This function is a blocking I/O operation.

   See http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#partitionsFor(java.lang.String)

  Usage :

  (list-all-partitions consumer)
  ;; => [{:topic \"topic-b\",
  ;;      :partition 2,
  ;;      :leader {:id 1, :host \"172.17.0.4\", :port 9092},
  ;;      :replicas [{:id 1, :host \"172.17.0.4\", :port 9092}],
  ;;      :in-sync-replicas [{:id 1, :host \"172.17.0.4\", :port 9092}]}
  ;;     {:topic \"topic-b\",
  ;;      :partition 1,
  ;;      :leader {:id 3, :host \"172.17.0.5\", :port 9094},
  ;;      :replicas [{:id 3, :host \"172.17.0.5\", :port 9094}],
  ;;      :in-sync-replicas [{:id 3, :host \"172.17.0.5\", :port 9094}]}
  ;;     {:topic \"topic-b\",
  ;;      :partition 0,
  ;;      :leader {:id 2, :host \"172.17.0.3\", :port 9093},
  ;;      :replicas [{:id 2, :host \"172.17.0.3\", :port 9093}],
  ;;      :in-sync-replicas [{:id 2, :host \"172.17.0.3\", :port 9093}]}]
"
  [^KafkaConsumer consumer topic]
  (mapv to-clojure  (.partitionsFor consumer topic)))


(defn pause
  "Stops messages being consumed from the given partitions.
   This takes effect on the next call on the messages function
   See http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#pause(org.apache.kafka.common.TopicPartition...)

  Usage:

  (pause consumer {:topic \"topic-a\" :partition 2}
                  {:topic \"topic-b\" :partition 0})
  "
  [^KafkaConsumer consumer tp-seq]
  (->> (map map->topic-partition tp-seq)
       (into-array TopicPartition)
       (.pause consumer)))


(defn resume
  "Resumes messages being consumed from the given partitions.
   This takes effect on the next call on the messages function
   See http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#resume(org.apache.kafka.common.TopicPartition...)

  Usage:

  (resume consumer {:topic \"topic-a\" :partition 2}
                   {:topic \"topic-b\" :partition 0})
  "
  [^KafkaConsumer consumer tp-seq]
  (->> (map map->topic-partition tp-seq)
       (into-array TopicPartition)
       (.resume consumer)))


(defn metrics
  "Returns a sequence of maps representing all the consumer's internal metrics.
   Each map contains information about metric-group (:group), metric-name (:name),
   metric-description (:description), metric-tags (:tags) and metric-value (:value)

  Usage :

  (metrics consumer)
  ;; => [{:group \"consumer-coordinator-metrics\",
  ;;      :name \"sync-time-max\",
  ;;      :description \"The max time taken for a group sync\",
  ;;      :tags {\"client-id\" \"consumer-3\"},
  ;;      :value 0.0}
  ;;     {:group \"consumer-fetch-manager-metrics\",
  ;;      :name \"bytes-consumed-rate\",
  ;;      :description \"The average number of bytes consumed per second\",
  ;;      :tags {\"client-id\" \"consumer-3\"},
  ;;      :value 0.0}]
  "
  [^KafkaConsumer consumer]
  (metrics->map (.metrics consumer)))
