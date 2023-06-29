package main

import (
	"encoding/json"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jun98427/go-kafka-example/pkg"
	"github.com/jun98427/go-kafka-example/schema"
	"log"
)

func main() {
	var i int = 0
	publisher := pkg.KafkaPublisher()

	for i = 0; i < 5; i++ {
		//var topic string
		//fmt.Print("Enter topic name: ")
		//if _, err := fmt.Scanln(&topic); err != nil {
		//	log.Fatal(err)
		//}
		//
		//fmt.Println("topic:", topic)
		topic := "test"
		content := fmt.Sprintf("message %d", i)
		//r := bufio.NewReader(os.Stdin)
		//fmt.Print("Enter message content: ")
		//content, _ := r.ReadString('\n')
		//content = strings.Replace(content, "\n", "", -1)

		msg, err := json.Marshal(schema.Message{Content: content})
		if err != nil {
			log.Fatal(err)
		}

		if err := publisher.Publish(topic, message.NewMessage(watermill.NewUUID(), msg)); err != nil {
			log.Fatal(err)
		}

		//if content == "exit" {
		//	break
		//}
	}

}
