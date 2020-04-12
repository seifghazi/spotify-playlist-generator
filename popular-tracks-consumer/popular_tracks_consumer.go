package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spotify-playlist-generator/tracks-producer/auth"
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
	var results []spotify.ID
	trackIDs := make(map[spotify.ID]bool)
	trackURIs := make(chan spotify.ID)

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
					var track spotify.FullTrack
					json.Unmarshal(msg.Value, &track)
					trackURIs <- track.ID
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
			if _, exists := trackIDs[trackURI]; !exists {
				results = append(results, trackURI)
				trackIDs[trackURI] = true
			}
		}
	}()

	wg.Wait()
	close(tracks)

	// authenticates user and stores tokens
	client, err := auth.Authenticate()

	if err != nil {
		log.Fatal(err)
	}

	app.client = client

	user, err := client.CurrentUser()
	if err != nil {
		log.Fatalln("Error getting user " + err.Error())
	}

	playlist, err := app.client.CreatePlaylistForUser(user.ID, "Automatic Bangers", "All these bangers are brought to you by Kafka", true)
	if err != nil {
		log.Fatalln("Error creating playlist: " + err.Error())
	}

	_, err = app.client.AddTracksToPlaylist(playlist.ID, results...)
	if err != nil {
		log.Fatalln("shit")
	}

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
