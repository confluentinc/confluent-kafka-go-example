# confluent-kafka-go-example

Example application using the confluent-kafka-go client.

## How to build and run

On OSX or glibc-based Linux (centos, ubuntu, ..):

    $ go run main.go mybroker mytopic

On Alpine:

    $ go run -tags musl main.go mybroker mytopic

