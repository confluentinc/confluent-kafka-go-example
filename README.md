# confluent-kafka-go-example

Example application using the confluent-kafka-go client.

This example is show-casing the experimental confluent-kafka-go-dev client
that bundles all required dependencies (librdkafka with friends),
allowing the client to be built and deployed without having to install
librdkafka first.


## How to build and run

On OSX or glibc-based Linux (centos, ubuntu, ..):

    $ go run main.go mybroker mytopic

On Alpine:

    $ go run -tags musl main.go mybroker mytopic

