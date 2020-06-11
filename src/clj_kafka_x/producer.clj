(ns ^{:doc "Clojure interface for Kafka Producer API. For
  complete JavaDocs, see:
  http://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/producer/package-summary.html"}
    clj-kafka-x.producer
  (:refer-clojure :exclude [send flush])
  (:require [clj-kafka-x.data :refer :all])
  (:import [java.util.concurrent Future TimeUnit TimeoutException]
           [org.apache.kafka.clients.producer Callback Producer KafkaProducer ProducerRecord RecordMetadata]
           [org.apache.kafka.common Metric MetricName]
           (org.apache.kafka.common.serialization Serializer ByteArraySerializer StringSerializer)
           (java.util Map)))



(defn- map-future-val
  [^Future fut f]
  (reify
    Future
    (cancel [_ interrupt?] (.cancel fut interrupt?))
    (get [_] (f (.get fut)))
    (get [_ timeout unit] (f (.get fut timeout unit)))
    (isCancelled [_] (.isCancelled fut))
    (isDone [_] (.isDone fut))))



(defn string-serializer [] (StringSerializer.))
(defn byte-array-serializer [] (ByteArraySerializer.))

(defn producer
  "Takes a map of config options and returns a `KafkaProducer` for publishing records to Kafka.

  NOTE `KafkaProducer` instances are thread-safe and should generally be shared for best performance.

  For more information and available config options,
  see: http://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
       http://kafka.apache.org/documentation.html#producerconfigs


  Usage:

;; Created using just a map of configs, in this case the keys
;; bootstrap.servers value.serializer and key.serializer are required
 (producer {\"bootstrap.servers\" \"localhost:9092\"
            \"value.serializer\" \"org.apache.kafka.common.serialization.StringSerializer\"
            \"key.serializer\" \"org.apache.kafka.common.serialization.StringSerializer\"})

;; Created using a map of configs and the serializers for keys and values.
 (producer {\"bootstrap.servers\" \"localhost:9092\"} (string-serializer) (string-serializer))

;; KafkaProducer should be closed when not used anymore, as it's closeable,
;; it can be used in the with-open macro
  (def config {\"bootstrap.servers\" \"localhost:9092\"})
  (with-open [p (producer config (string-serializer) (string-serializer))]
    (-> (send p (record \"topic-a\" \"Hello World\"))
        (.get)))
  "

  ([^Map config]
   (KafkaProducer. config))
  ([^Map config ^Serializer key-serializer ^Serializer value-serializer]
   (KafkaProducer. config key-serializer value-serializer)))

(defn record
  "Return a record that can be published to Kafka using [[send]]."
  ([topic value]
   (ProducerRecord. topic value))
  ([topic key value]
   (ProducerRecord. topic key value))
  ([topic partition key value]
   (ProducerRecord. topic partition key value)))

(defn send
  "Asynchronously send a record to Kafka. Returns a `Future` of a map
  with `:topic`, `:partition` and `:offset` keys. Optionally provide
  a callback fn that will be called when the operation completes.
  Callback should be a fn of two arguments, a map as above, and an
  exception. Exception will be nil if operation succeeded.

  See: http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#send(org.apache.kafka.clients.producer.ProducerRecord)

  Usage:

  ;;To send the message asynchronously and return a Future
  (send producer (record \"topic-a\" \"Test message 1\"))
  ;; => #object[string representation of future object]

  ;;To send message synchronously, deref the returned Future
  @(send producer (record \"topic-a\" \"Test message 2\"))
  ;; => {:topic \"topic-a\", :partition 4, :offset 0}

  ;;To send the message asynchronously and provide a callback
  ;;returns the future.
  (send producer (record \"topic-a\" \"Test message 3\") #(println \"Metadata->\" %1 \"Exception->\" %2))
  ;; => #object[string representation of future object]
  ;; Metadata-> {:topic topic-unknown, :partition 4, :offset 1} Exception-> nil
  "
  ([^Producer producer record]
   (let [fut (.send producer record)]
     (map-future-val fut to-clojure)))
  ([^Producer producer record callback]
   (let [fut (.send producer record (reify Callback
                                      (onCompletion [_ metadata exception]
                                        (callback (and metadata (to-clojure metadata)) exception))))]
     (map-future-val fut to-clojure))))

(defn flush
  "See: http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#flush()"
  [^Producer producer]
  (.flush producer))

(defn close
  "Like `.close`, but with a default time unit of ms for the arity with timeout.

  See:

  - http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#close()
  - http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#close(long,%20java.util.concurrent.TimeUnit)"
  ([^Producer producer]
   (.close producer))
  ([^Producer producer timeout-ms]
   (.close producer timeout-ms TimeUnit/MILLISECONDS)))

(defn partitions
  "Returns a sequence of maps which represent information about each partition of the
  specified topic.

  Usage :

  (partitions producer \"topic-a\")
  ;; => [{:topic \"topic-a\",
  ;;      :partition 2,
  ;;      :leader {:id 2, :host \"172.17.0.3\", :port 9093},
  ;;      :replicas [{:id 2, :host \"172.17.0.3\", :port 9093}
  ;;                 {:id 3, :host \"172.17.0.5\", :port 9094}],
  ;;      :in-sync-replicas [{:id 2, :host \"172.17.0.3\", :port 9093}
  ;;                         {:id 3, :host \"172.17.0.5\", :port 9094}]}
  ;;     {:topic \"topic-a\",
  ;;      :partition 1,
  ;;      :leader {:id 1, :host \"172.17.0.4\", :port 9092},
  ;;      :replicas [{:id 1, :host \"172.17.0.4\", :port 9092}
  ;;                 {:id 2, :host \"172.17.0.3\", :port 9093}],
  ;;      :in-sync-replicas [{:id 1, :host \"172.17.0.4\", :port 9092}
  ;;                         {:id 2, :host \"172.17.0.3\", :port 9093}]}]
  "
  [^Producer producer topic]
  (mapv to-clojure (.partitionsFor producer topic)))

(defn metrics
  "Returns a sequence of maps representing all the producer's internal metrics.
   Each map contains information about metric-group (:group), metric-name (:name),
   metric-description (:description), metric-tags (:tags) and metric-value (:value)

  Usage :

  (metrics producer)
  ;; => [{:group \"producer-metrics\",
  ;;      :name \"record-queue-time-avg\",
  ;;      :description \"The average time in ms record batches spent in the record accumulator.\",
  ;;      :tags {\"client-id\" \"producer-2\"},
  ;;      :value 0.1}
  ;;     {:group \"producer-metrics\",
  ;;      :name \"outgoing-byte-rate\",
  ;;      :description \"The average number of outgoing bytes sent per second to all servers.\",
  ;;      :tags {\"client-id\" \"producer-2\"},
  ;;      :value 31.668376965849703}
  ;;     {:group \"producer-node-metrics\",
  ;;      :name \"response-rate\",
  ;;      :description \"The average number of responses received per second.\",
  ;;      :tags {\"client-id\" \"producer-2\", \"node-id\" \"node-3\"},
  ;;      :value 0.23866348448687352}]
  "
  [^Producer producer]
  (metrics->map (.metrics producer))
  )
