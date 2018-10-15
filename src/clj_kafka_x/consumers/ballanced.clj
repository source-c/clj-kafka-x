(ns clj-kafka-x.consumers.ballanced
  (:require [clojure.string :refer [join]])
  (:import (java.util.concurrent Executors)
           (org.apache.kafka.clients.consumer KafkaConsumer Consumer ConsumerConfig ConsumerRecords)
           (java.util Properties)
           (clojure.lang PersistentArrayMap)))

(defrecord KafkaMessage [topic partition offset key message])

(defn- process-msg [fn-proc ^KafkaMessage msg id]
  (when (fn? fn-proc) (fn-proc (.message msg) id)))

(defn- consume-messages
  "Continually consume messages from a Kafka topic and write message value to stdout."
  ([stream thread-num spec]
   (consume-messages stream thread-num spec (:id spec)))
  ([stream thread-num spec id]
   (let [it (.iterator ^ConsumerRecords stream)]
     (while (.hasNext it)
       (let [msg (.next it)
             kmsg (KafkaMessage.
                    (.topic msg)
                    (.partition msg)
                    (.offset msg)
                    (.key msg)
                    (.value msg))
             prc @(resolve (:processor spec))]

         (process-msg prc kmsg id)))
     (.close stream))))

(defn- start-consumer-threads
  "Starts a thread for each stream."
  [thread-pool kafka-streams spec]
  (loop [streams kafka-streams index 0]
    (when (seq streams)
      (.submit thread-pool (cast Callable
                                 #(consume-messages (first streams) index spec)))
      (recur (rest streams) (inc index)))))

(defn consume-topic
  "Pull messages from a Kafka topic using the Consumer instance"
  ([consumer topic pool poolsize]
   (consume-topic consumer topic pool poolsize {:decoder nil :schema nil}))
  ([consumer topic pool poolsize ^PersistentArrayMap spec]
   (let [consumer-map (.createMessageStreams
                        consumer
                        {topic (Integer. poolsize)})
         kafka-streams (.get consumer-map topic)]
     ;; Connect and start listening for messages on Kafka
     (start-consumer-threads pool kafka-streams spec))
   consumer))

(defn- ->zklist
  ([blist]
   (->zklist blist nil))
  ([blist prefix]
   (let [zklist (join "," blist)]
     (if-not (nil? prefix)
       (join "/" [zklist prefix])
       zklist))))

;; ---------- Consumer ----------
(defn- create-consumer-config
  "Returns a configuration for a Kafka client."
  [config zklist grname]
  (let [props (Properties.)
        spec (:spec (:consumer config))
        {:keys [deserializer zktimeout
                zksync zkautocommit]} spec]
    (doto props
      (.put "zookeeper.connect" zklist)
      (.put "group.id" grname)
      (.put "deserializer.class" deserializer)
      (.put "zookeeper.session.timeout.ms" zktimeout)
      (.put "zookeeper.sync.time.ms" zksync)
      (.put "auto.commit.interval.ms" zkautocommit))
    (ConsumerConfig. props)))

(defn- create-consumer
  ([config blist prefix]
   (create-consumer blist prefix (:group (:consumer config))))
  ([config blist prefix grname]
   (KafkaConsumer.
     (create-consumer-config config (->zklist blist prefix) grname))))

(defn- shutdown-topic [topic obj]
  (when-let [inst (:instance obj)]
    (.shutdown inst))
  (when-let [pool (:pool obj)]
    (.shutdown pool))
  topic)

(defn- run-consumers
  "
  Runs consumers for multiple topics
  Requires:
   - external atom to store ran consumers
   - hashmap of topic specs
  Returns updated atom
  "
  [group topics-list storage kafka-spec]
  (doseq [T (keys topics-list)]
    (let [{:keys [zkpool zkpref]} kafka-spec
          topic-spec (get topics-list T)
          psize (:poolsize topic-spec)
          tpool (Executors/newFixedThreadPool psize)
          consumer (create-consumer zkpool zkpref group)]
      (try (swap! storage merge
                  {T {:instance (consume-topic
                                  consumer T
                                  tpool psize
                                  (get topics-list T))
                      :pool     tpool}})
           (catch Exception e (do (shutdown-topic T {:pool tpool}))))))
  storage)
