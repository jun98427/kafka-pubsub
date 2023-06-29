package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jun98427/go-kafka-example/pkg"
	"github.com/jun98427/go-kafka-example/schema"
	"log"
	"os"
	"strings"
)

func main() {
	var topic, group string
	r := bufio.NewReader(os.Stdin)

	fmt.Print("Enter topic name: ")
	if _, err := fmt.Scanln(&topic); err != nil {
		log.Fatal(err)
	}

	fmt.Print("Enter group name: ")
	group, _ = r.ReadString('\n')
	group = strings.Replace(group, "\n", "", -1)

	fmt.Println("topic:", topic)

	consumer := pkg.KafkaSubscriber(group)
	sub, err := consumer.Subscribe(context.Background(), topic)
	if err != nil {
		log.Fatal(err)
	}

	for {
		m := <-sub
		m.Ack()
		var msg schema.Message
		if err := json.Unmarshal(m.Payload, &msg); err != nil {
			log.Fatal(err)
		}

		fmt.Println("message:", msg.Content)
		if msg.Content == "exit" {
			break
		}
	}
}
