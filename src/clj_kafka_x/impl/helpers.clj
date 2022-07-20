(ns clj-kafka-x.impl.helpers
  (:import (java.util Arrays)
           (org.apache.kafka.common.config ConfigDef ConfigDef$ConfigKey)))


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
