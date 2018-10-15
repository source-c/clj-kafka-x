{:dev      {:plugins [[lein-ancient "0.6.15"]]}
 :docs     {:plugins [[lein-pprint "1.2.0"]
                      [lein-codox "0.10.3"]
                      [org.timmc/nephila "0.3.0"]]}
 :provided {:dependencies [[org.clojure/clojure "1.9.0"]]}
 :uberjar  {:aot :all :jvm-opts #=(eval
                                    (concat ["-Xmx1G"]
                                      (let [version (System/getProperty "java.version")
                                            [major _ _] (clojure.string/split version #"\.")]
                                        (if (>= (Integer. major) 9)
                                          ["--add-modules" "java.xml.bind"]
                                          []))))}}
