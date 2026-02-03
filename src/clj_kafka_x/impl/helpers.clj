(ns clj-kafka-x.impl.helpers
  (:require [clj-kafka-x.data :refer [to-clojure metrics->map map->topic-partition]])
  (:import (java.util Arrays)
           (org.apache.kafka.common.config ConfigDef ConfigDef$ConfigKey)
           (org.apache.kafka.common TopicPartition)))


(defn- coerce-value [t v]
  (case t
    :SHORT (when (number? v) (short v))
    :INT (when (number? v) (int v))
    :LONG (when (number? v) (long v))
    :DOUBLE (when (number? v) (double v))
    :LIST (when (coll? v)
            (->> v
                 (into-array String)
                 Arrays/asList))
    nil))

(defn- coerce-config-entry [config-def [k v]]
  (let [k (cond-> k keyword? name)]
    (or
      (when-let [^ConfigDef$ConfigKey key (get config-def k)]
        (let [t (-> (.type key) str keyword)]
          (when-let [v (coerce-value t v)]
            [k v])))
      [k v])))

(defn coerce-config [^ConfigDef def conf]
  (->> conf
       (map (partial coerce-config-entry (into {} (.configKeys def))))
       (into {})))

;; Shared client helpers to avoid duplication between producer and consumer

(defn client-metrics
  "Extracts metrics from a Kafka client (producer or consumer) and returns
   a sequence of maps with :group, :name, :description, :tags, and :value keys."
  [client]
  (metrics->map (.metrics client)))

(defn client-partitions-for
  "Returns partition info for a topic from a Kafka client (producer or consumer)
   as a vector of maps."
  [client topic]
  (mapv to-clojure (.partitionsFor client topic)))

(defn ->topic-partition-array
  "Converts a sequence of topic-partition maps to a TopicPartition array."
  [tp-seq]
  (->> (map map->topic-partition tp-seq)
       (into-array TopicPartition)))
