# clj-kafka-x tests

This directory contains tests for the clj-kafka-x library.

## Running Tests

To run all tests:

```
lein with-profile dev test
```

To run specific tests:

```
lein with-profile dev test clj-kafka-x.producer-test
lein with-profile dev test clj-kafka-x.consumers.simple-test
lein with-profile dev test clj-kafka-x.data-test
lein with-profile dev test clj-kafka-x.impl.helpers-test
```

## Test Coverage

To generate test coverage reports:

```
lein with-profile dev cloverage
```

The coverage report will be generated in `target/coverage/`.

## Test Structure

- `test_helpers.clj`: Contains helper functions for testing, including mock producer and consumer setup
- `producer_test.clj`: Tests for the producer functionality
- `consumers/simple_test.clj`: Tests for the consumer functionality
- `data_test.clj`: Tests for data conversion functionality
- `impl/helpers_test.clj`: Tests for helper functions
- `test_runner.clj`: A standalone test runner

## Notes

The tests use mock Kafka producers and consumers to avoid requiring a running Kafka instance. This allows for faster and more reliable tests.