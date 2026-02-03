{:dev      {:dependencies [[midje "1.10.10"]
                           [org.slf4j/slf4j-simple "2.0.17"]
                           [org.mockito/mockito-core "5.21.0"]]
            :test-paths   ["test"]
            :plugins      [[lein-cloverage "1.2.4"]]}
 :docs     {:plugins [[lein-codox "0.10.8"]
                      [org.timmc/nephila "0.3.0"]]}
 :provided {:dependencies [[org.clojure/clojure "1.12.4"]]}
 :jar      {:aot :all}}
