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
	app                 = &App{}
	popularTrackIDs     = make(map[spotify.ID]bool)
	lessPopularTrackIDs = make(map[spotify.ID]bool)
	// trackURIs = make(chan spotify.ID)
	wg sync.WaitGroup
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	consumer, err := kafkaConsumerSetup()
	if err != nil {
		log.Fatal(err)
	}

	popularTracksChannel := make(chan []spotify.ID)
	getTrackIDs(consumer, "popular-tracks", popularTracksChannel, popularTrackIDs)
	listOfPopularTrackIDs := <-popularTracksChannel
	fmt.Println("Before wait")
	fmt.Println(listOfPopularTrackIDs)
	wg.Wait()

	// lessPopularTracksChannel := make(chan spotify.ID)
	// listofLessPopularTrackIDs := getTrackIDs(consumer, "less-popular-tracks", lessPopularTracksChannel, lessPopularTrackIDs)

	fmt.Println("After wait")
	fmt.Println(listOfPopularTrackIDs)
	// close(popularTracksChannel)
	// close(lessPopularTracksChannel)

	// authenticates user and stores tokens
	client, err := auth.Authenticate()

	if err != nil {
		log.Fatal(err)
	}

	app.client = client

	err = app.AddTracksToPlaylist("Bangers", "All this heat is brought to you by Kafka", listOfPopularTrackIDs...)
	if err != nil {
		log.Fatalln("Error adding tracks to playlist: " + err.Error())
	}

	// err = app.AddTracksToPlaylist("Potential Bangers", "All this heat is brought to you by Kafka", listofLessPopularTrackIDs...)
	if err != nil {
		log.Fatalln("Error adding tracks to playlist: " + err.Error())
	}

	// user, err := client.CurrentUser()

	// playlist, err := app.client.CreatePlaylistForUser(user.ID, "Automatic Bangers", "All these bangers are brought to you by Kafka", true)
	// if err != nil {
	// 	log.Fatalln("Error creating playlist: " + err.Error())
	// }

	// _, err = app.client.AddTracksToPlaylist(playlist.ID, listOfPopularTrackIDs...)
	// if err != nil {
	// 	log.Fatalln("shit")
	// }

}

func kafkaConsumerSetup() (sarama.Consumer, error) {
	brokers := []string{"127.0.0.1:9092"}
	consumer, err := sarama.NewConsumer(brokers, nil)

	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func getTrackIDs(consumer sarama.Consumer, topic string, trackIDChannel chan []spotify.ID, consumedIDs map[spotify.ID]bool) []spotify.ID {
	// collects results
	var results []spotify.ID
	// go func() {
	// 	for trackIDs := range trackIDChannel {
	// 		results = trackIDs
	// 	}
	// }()

	consumeTracks(consumer, topic, trackIDChannel, consumedIDs)

	return results
}

func consumeTracks(consumer sarama.Consumer, topic string, trackIDChannel chan []spotify.ID, consumedIDs map[spotify.ID]bool) {
	partitionList, _ := consumer.Partitions(topic)
	initialOffset := sarama.OffsetOldest
	timer := time.NewTimer(3 * time.Second)

	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, initialOffset)

		if err != nil {
			log.Fatal(err.Error())
		}

		wg.Add(1)
		go func(pc sarama.PartitionConsumer, partitionNum int32) {
			var results []spotify.ID
		ConsumerLoop:
			for {
				select {
				case msg := <-pc.Messages():
					var track spotify.FullTrack
					json.Unmarshal(msg.Value, &track)
					if _, exists := consumedIDs[track.ID]; !exists {
						results = append(results, track.ID)
						consumedIDs[track.ID] = true
					}
					// fmt.Println(string(msg.Value))
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(3 * time.Second)
				case <-timer.C:
					log.Println("Timeout")
					trackIDChannel <- results
					break ConsumerLoop
				}
			}
			log.Printf("Partition #%d status = donezoo", partitionNum)
			wg.Done()
		}(pc, partition)
	}
}

func (a *App) AddTracksToPlaylist(name, description string, trackIDs ...spotify.ID) error {
	user, err := a.client.CurrentUser()
	if err != nil {
		return err
	}

	playlist, err := app.client.CreatePlaylistForUser(user.ID, name, description, true)
	if err != nil {
		return err
	}

	_, err = app.client.AddTracksToPlaylist(playlist.ID, trackIDs...)
	if err != nil {
		return err
	}

	return nil
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
