(ns clj-kafka-x.test-runner
  (:require [clojure.test :refer [run-tests successful?]]
            [clj-kafka-x.producer-test]
            [clj-kafka-x.data-test]
            [clj-kafka-x.impl.helpers-test]
            [clj-kafka-x.consumers.simple-test])
  (:gen-class))

(defn run-all-tests []
  (let [results (run-tests 'clj-kafka-x.producer-test
                          'clj-kafka-x.data-test
                          'clj-kafka-x.impl.helpers-test
                          'clj-kafka-x.consumers.simple-test)]
    (System/exit (if (successful? results) 0 1))))

(defn -main [& args]
  (run-all-tests))