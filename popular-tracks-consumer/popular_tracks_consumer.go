package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/zmb3/spotify"
)

type App struct {
	client *spotify.Client
}

var (
	app    = &App{}
	tracks = make(chan spotify.FullTrack)
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	brokers := []string{"127.0.0.1:9092"}

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatal(err)
	}

	topic := "popular-tracks"                        //e.g. user-created-topic
	partitionList, err := consumer.Partitions(topic) //get all partitions
	if err != nil {
		fmt.Println(err.Error())
	}

	var wg sync.WaitGroup
	var results []string
	trackURIs := make(chan string)

	initialOffset := sarama.OffsetOldest //offset to start reading message from
	timer := time.NewTimer(3 * time.Second)
	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, initialOffset)

		if err != nil {
			log.Fatal(err.Error())
		}

		wg.Add(1)
		go func(pc sarama.PartitionConsumer, partitionNum int32) {
		ConsumerLoop:
			for {
				select {
				case msg := <-pc.Messages():
					fmt.Println("New message babyyyyy")
					trackURIs <- string(msg.Value)
					// fmt.Println(string(msg.Value))
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(3 * time.Second)
				case <-timer.C:
					log.Println("Timeout")
					break ConsumerLoop
				}
			}
			log.Printf("Partition #%d status = donezoo", partitionNum)
			wg.Done()
		}(pc, partition)
	}

	// collects results
	go func() {
		for trackURI := range trackURIs {
			results = append(results, trackURI)
		}
	}()

	wg.Wait()
	close(tracks)
	fmt.Println(len(results))

	// authenticates user and stores tokens
	// client, err := auth.Authenticate()

	// if err != nil {
	// 	log.Fatal(err)
	// }

	// app.client = client
}

func loadConfig() (map[string]interface{}, error) {
	var config map[string]interface{}
	file, err := os.Open("popular-tracks-consumer/config.json")

	if err != nil {
		return nil, fmt.Errorf("Error opening config file: %s", err.Error())
	}

	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)

	if err != nil {
		return nil, fmt.Errorf("Error decoding config file: %s", err.Error())
	}

	return config, nil
}
