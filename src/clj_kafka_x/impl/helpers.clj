(ns clj-kafka-x.impl.helpers
  (:import (clojure.lang MapEntry IRecord IMapEntry)
           (java.util Map)))

(defn- walk
  [inner outer form]
  (cond
    (list? form) (outer (apply list (map inner form)))
    (instance? IMapEntry form)
    (outer (MapEntry/create (inner (key form)) (inner (val form))))
    (seq? form) (outer (doall (map inner form)))
    (instance? IRecord form)
    (outer (reduce (fn [r x] (conj r (inner x))) form form))
    (coll? form) (outer (into (empty form) (map inner form)))
    :else (outer form)))

(defn- postwalk
  [f form]
  (walk (partial postwalk f) f form))

(defn- stringify-keys
  [m]
  (let [f (fn [[k v]] (if (keyword? k) [(name k) v] [k v]))]
    (postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))

(defn update-in-when
  [m k f & args]
  (if (not= ::not-found (get-in m k ::not-found))
    (apply update-in m k f args)
    m))

(defn- short! [v]
  (if (number? v)
    (short v)
    (condp instance? v
      String (try
               (-> v str Short/parseShort)
               (catch NumberFormatException _ nil))
      nil)))

(defn- int! [v]
  (if (number? v)
    (int v)
    (condp instance? v
      String (try
               (-> v str Integer/parseInt)
               (catch NumberFormatException _ nil))
      nil)))

(defn safe-config ^Map [^Map config]
  (if (map? config)
    (-> config
        stringify-keys
        ;; common
        (update-in-when ["request.timeout.ms"] int!)
        (update-in-when ["receive.buffer.bytes"] int!)
        (update-in-when ["metrics.num.samples"] int!)
        ;; producer
        (update-in-when ["send.buffer.bytes"] int!)
        (update-in-when ["retries"] int!)
        (update-in-when ["batch.size"] int!)
        (update-in-when ["delivery.timeout.ms"] int!)
        (update-in-when ["max.request.size"] int!)
        (update-in-when ["max.in.flight.requests.per.connection"] int!)
        (update-in-when ["sasl.login.connect.timeout.ms"] int!)
        (update-in-when ["sasl.login.read.timeout.ms"] int!)
        (update-in-when ["sasl.login.refresh.buffer.seconds"] short!)
        (update-in-when ["sasl.login.refresh.min.period.seconds"] short!)
        (update-in-when ["sasl.oauthbearer.clock.skew.seconds"] int!)
        (update-in-when ["transaction.timeout.ms"] int!)
        ;; consumer
        (update-in-when ["fetch.min.bytes"] int!)
        (update-in-when ["fetch.max.bytes"] int!)
        (update-in-when ["fetch.max.wait.ms"] int!)
        (update-in-when ["heartbeat.interval.ms"] int!)
        (update-in-when ["max.partition.fetch.bytes"] int!)
        (update-in-when ["session.timeout.ms"] int!)
        (update-in-when ["max.poll.interval.ms"] int!)
        (update-in-when ["max.poll.records"] int!)
        (update-in-when ["auto.commit.interval.ms"] int!)
        (update-in-when ["fetch.max.wait.ms"] int!))
    {}))
