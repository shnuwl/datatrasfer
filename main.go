package main

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"
	"utils"
)

func main() {

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// init consumer
	brokers := []string{"10.0.0.1:9092", "10.0.0.2:9092"}
	topics := []string{"topic"}
	consumer, err := cluster.NewConsumer(brokers, "group", topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	pending := make(map[string][]string)
	t := time.Tick(5 * time.Second)

	// consume messages, watch errors and notifications
	for {
		select {
		case msg, more := <-consumer.Messages():
			if more {
				//fmt.Fprintf(os.Stdout, "%s/%d/%d\t\n", msg.Topic, msg.Partition, msg.Offset)
				//fmt.Println(strings.SplitAfterN(string(msg.Value), "+", 3)[:2])
				fr := strings.SplitAfterN(string(msg.Value), "+", 3)[2]
				data := utils.Extract_File(fr)
				utils.Process_file(data, pending)
				consumer.MarkOffset(msg, "") // mark message as processed
			}
		case <-t:
			utils.Commit_to_s3(&pending)
			pending = make(map[string][]string)
		case err, more := <-consumer.Errors():
			if more {
				log.Printf("Error: %s\n", err.Error())
			}
		case ntf, more := <-consumer.Notifications():
			if more {
				log.Printf("Rebalanced: %+v\n", ntf)
			}
		case <-signals:
			return
		}
	}
}
