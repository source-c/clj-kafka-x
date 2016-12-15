(ns clj-kafka-x.data
  (:import [java.util HashMap Map Properties]
           [org.apache.kafka.clients.consumer ConsumerRecord ConsumerRecords OffsetAndMetadata]
           org.apache.kafka.clients.producer.RecordMetadata
           [org.apache.kafka.common Metric MetricName Node PartitionInfo TopicPartition]

           (kafka.consumer Consumer ConsumerConfig KafkaStream)
           (kafka.message MessageAndMetadata)))

(defprotocol ToClojure
  ""
  (to-clojure [x] "Converts type to Clojure structure"))


(extend-protocol ToClojure

  nil
  (to-clojure [x] nil)

  TopicPartition
  (to-clojure [x]
    {:topic (.topic x)
     :partition (.partition x)})

  ConsumerRecord
  (to-clojure [x]
    {:topic (.topic x)
     :partition (.partition x)
     :offset (.offset x)
     :key (.key x)
     :value (.value x)})

  MessageAndMetadata
  (to-clojure [x]
    {:topic (.topic x)
     :partition (.partition x)
     :offset (.offset x)
     :key (.key x)
     :value (.message x)}) ;; FIME: :value/:message

  ConsumerRecords
  (to-clojure [x]
    (mapv to-clojure x))

  OffsetAndMetadata
  (to-clojure [x]
    {:offset (.offset x)
     :metadata (.metadata x)})

  Node
  (to-clojure [x]
    {:id (.id x)
     :host (.host x)
     :port (.port x)})

  PartitionInfo
  (to-clojure [x]
    {:topic (.topic x)
     :partition (.partition x)
     :leader (to-clojure (.leader x))
     :replicas (mapv to-clojure (.replicas x))
     :in-sync-replicas  (mapv to-clojure (.inSyncReplicas x))})

  RecordMetadata
  (to-clojure [x]
    {:topic (.topic x)
     :partition (.partition x)
     :offset (.offset x)})

  MetricName
  (to-clojure [x]
    {:name (.name x)
     :description (.description x)
     :group (.group x)
     :tags (reduce  (fn [m [t t-val]] (assoc m t t-val)) {} (.tags x))})

  Metric
  (to-clojure [x]
    {:metric-name (to-clojure (.metricName x))
     :value (.value x)}))

(defn ^Properties map->properties
  [^Map m]
  (let [p (Properties.)]
    (doseq [[k ^String v] m]
      (.setProperty p (name k) v))
    p))


(defn ^TopicPartition map->topic-partition [{:keys [topic partition] :as m}]
  (if (or (nil? topic) (nil? partition))
    (throw (ex-info "Provided map is missing topic or partition keys" m))
    (TopicPartition. topic partition)))

(defn ^OffsetAndMetadata map->offset-metadata [{:keys [offset metadata] :as m}]
  (if (or (nil? offset) (nil? metadata))
    (throw (ex-info "Provided map is missing offset or metadata keys" m))
    (OffsetAndMetadata. offset metadata)))

;;NOT happy with this function name
(defn tp-om-map->map
  "Takes a java.util.Map made of TopicPartition as keys and OffsetAndMetadata as values,
   converts them to the following clojure equivalent data structure

  {{:topic \"test\", :partition 77} {:offset 34, :metadata \"data data\"},
   {:topic \"prod\", :partition 4} {:offset 24, :metadata \"more data\"},
   {:topic \"dev\", :partition 1} {:offset 234, :metadata \"loads of data\"},
   {:topic \"dev\", :partition 7} {:offset 23, :metadata \"mega data\"}}
  "
  [^Map tp-om]
  (let [reduce-fn (fn [m [^TopicPartition tp ^OffsetAndMetadata om]]
                    (assoc m (to-clojure tp) (to-clojure om)))]
    (reduce reduce-fn {} tp-om)))

;;NOT happy with this function name
(defn map->tp-om-map
  "Takes a Clojure map (see below for example) and converts it to a java.util.Map made of TopicPartition as keys and OffsetAndMetadata as values

  {{:topic \"test\", :partition 77} {:offset 34, :metadata \"data data\"},
   {:topic \"prod\", :partition 4} {:offset 24, :metadata \"more data\"},
   {:topic \"dev\", :partition 1} {:offset 234, :metadata \"loads of data\"},
   {:topic \"dev\", :partition 7} {:offset 23, :metadata \"mega data\"}}
  "
  [m]
  (let [tp-om-map (HashMap.)
        reduce-fn (fn [^Map m kv]
                    (.put m (map->topic-partition (first kv))
                          (map->offset-metadata (second kv)))
                    m)]
    (reduce reduce-fn tp-om-map  m)))


;;NOT happy with this function name
(defn str-pi-map->map
  "Takes a java.util.Map made of Strings as keys and java.util.List <PartitionInfo> as values,
   converts it to a Map with the keys being the topic name and the values being a vector of maps (each map representing information about a topic partition)
  e.g

  {
  \"topic-a\"
  [{:topic \"topic-a\",
   :partition 1,
   :leader {:id 1, :host \"172.17.0.3\", :port 9092},
   :replicas [{:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\", :port 9093} {:id 3, :host \"172.17.0.4\", :port 9094}],
   :in-sync-replicas [{:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\", :port 9093} {:id 3, :host \"172.17.0.4\", :port 9094}]}
  {:topic \"topic-a\",
   :partition 0,
   :leader {:id 3, :host \"172.17.0.4\", :port 9094},
   :replicas [{:id 3, :host \"172.17.0.4\", :port 9094} {:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\", :port 9093}],
   :in-sync-replicas [{:id 3, :host \"172.17.0.4\", :port 9094} {:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\", :port 9093}]}],

  \"topic-b\"
  [{:topic \"topic-b\",
   :partition 0,
   :leader {:id 3, :host \"172.17.0.4\", :port 9094},
   :replicas [{:id 3, :host \"172.17.0.4\", :port 9094} {:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\",
:port 9093}],
   :in-sync-replicas [{:id 3, :host \"172.17.0.4\", :port 9094} {:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\", :port 9093}]}]}
  "
  [^Map str-pi]
  (let [reduce-fn (fn [m [name pi-list]]
                    (assoc m name (mapv to-clojure pi-list)))]
    (reduce reduce-fn {} str-pi)))

(defn metrics->map
  "Returns a sequence of maps, with each map representing a metric.
   The composition of each map is :group :name :description :tags :value

  Usage :
  (metrics->map m)
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
  [m]
  (let [map-fn (fn [[^MetricName met-name ^Metric met]]
                 (let [mn-map (to-clojure met-name)
                       m-map (to-clojure met)]
                   {:group (:group mn-map)
                    :name (:name mn-map)
                    :description (:description mn-map)
                    :tags (:tags mn-map)
                    :value (:value m-map)}))]
    (mapv map-fn m)))
