// Example application using confluent-kafka-go-dev (static builds)
package main

/**
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"bufio"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	if len(os.Args) != 3 {
		_, libver := kafka.LibraryVersion()
		fmt.Fprintf(os.Stderr,
			"Example application (librdkafka v%s, linkage %s)\n",
			libver, kafka.LibrdkafkaLinkInfo)

		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	topic := os.Args[2]

	// Fail quickly for the sake of this demo.
	const messageTimeoutMs = 5000

	conf := kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"message.timeout.ms": messageTimeoutMs,
	}

	p, err := kafka.NewProducer(&conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	tp := kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}

	// Go-routine for handling delivery reports and client error events.
	go func(drs chan kafka.Event) {
		for ev := range drs {
			switch e := ev.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error != nil {
					fmt.Fprintf(os.Stderr, "%% Delivery error: %v\n", e.TopicPartition)
				} else {
					fmt.Fprintf(os.Stderr, "%% Delivered %v\n", e)
				}

			case kafka.Error:
				// Errors are typically informational, the
				// client will attempt to automatically recover.
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)

			default:
				fmt.Fprintf(os.Stderr, "%% Unhandled event %T ignored: %v\n", e, e)
			}
		}
	}(p.Events())

	fmt.Fprintf(os.Stderr, "%% Enter message to produce and hit enter\n")
	fmt.Fprintf(os.Stderr, "%% Ctrl-D to exit to flush messages and exit\n")

	reader := bufio.NewReader(os.Stdin)
	stdinChan := make(chan string)

	go func() {
		for true {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}

			line = strings.TrimSuffix(line, "\n")
			if len(line) == 0 {
				continue
			}

			stdinChan <- line
		}
		close(stdinChan)
	}()

	run := true

	for run == true {
		select {
		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			run = false

		case line, ok := <-stdinChan:
			if !ok {
				run = false
				break
			}

			msg := kafka.Message{
				TopicPartition: tp,
				Value:          ([]byte)(line),
			}

			p.ProduceChannel() <- &msg
		}
	}

	fmt.Fprintf(os.Stderr, "%% Flushing %d message(s)\n", p.Len())
	remaining := p.Flush(messageTimeoutMs + 1000)
	if remaining > 0 {
		fmt.Fprintf(os.Stderr,
			"%% Exiting with %d message(s) still in queue/transit\n",
			remaining)
	}

	p.Close()
}
