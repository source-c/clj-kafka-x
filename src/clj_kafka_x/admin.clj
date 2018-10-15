(ns clj-kafka-x.admin
  (:require [clj-kafka-x.data :refer [map->properties]])
  (:import kafka.admin.AdminUtils
           (kafka.admin RackAwareMode RackAwareMode$Enforced$)
           kafka.utils.ZkUtils))

(defn zk-utils
  ""
  [zk-url & {:keys [session-timeout connection-timeout security-enabled]
             :or   {session-timeout    1000
                    connection-timeout 1000
                    security-enabled   false}}]
  (ZkUtils/apply zk-url session-timeout connection-timeout (Boolean/valueOf security-enabled)))


(defn create-topic
  "Creates a topic using provided zk-utils, topic-name and optional key-values.
  The optional key-values are
  :partitions          --- number of topic partitions (default 1)
  :replication-factor  --- copies (inc original) of each partition (default 1)
  :topic-config        --- Topic specific configuration (default uses broker's default configs)
                           see http://kafka.apache.org/documentation.html#topic-config

  e.g

  (def z-utils (zk-utils \"localhost:2181\"))

  (create-topic z-utils \"topic-a\")

  (create-topic z-utils \"topic-b\" :topic-config {\"cleanup.policy\" \"compact\"})
  "
  [z-utils topic & {:keys [partitions replication-factor topic-config]
                    :or   {partitions         1
                           replication-factor 1
                           topic-config       nil}}]
  (AdminUtils/createTopic z-utils
                          topic
                          (int partitions)
                          (int replication-factor)
                          (map->properties topic-config)
                          RackAwareMode$Enforced$))

(defn topic-exists?
  "Returns true or false dependant on the existance of the given topic"
  [z-utils topic]
  (AdminUtils/topicExists z-utils topic))

(defn delete-topic
  "Deletes the given topic"
  [z-utils topic]
  (AdminUtils/deleteTopic z-utils topic))
