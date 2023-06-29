package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jun98427/go-kafka-example/pkg"
	"github.com/jun98427/go-kafka-example/schema"
	"log"
	"os"
	"strings"
)

func main() {
	for {
		var topic string
		fmt.Print("Enter topic name: ")
		if _, err := fmt.Scanln(&topic); err != nil {
			log.Fatal(err)
		}

		fmt.Println("topic:", topic)

		r := bufio.NewReader(os.Stdin)
		fmt.Print("Enter message content: ")
		content, _ := r.ReadString('\n')
		content = strings.Replace(content, "\n", "", -1)

		msg, err := json.Marshal(schema.Message{Content: content})
		if err != nil {
			log.Fatal(err)
		}

		publisher := pkg.KafkaPublisher()

		if err := publisher.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte(msg))); err != nil {
			log.Fatal(err)
		}

		if content == "exit" {
			break
		}
	}

}
