package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spotify-playlist-generator/popular-tracks-consumer/config"
	"github.com/spotify-playlist-generator/tracks-producer/auth"
	"github.com/zmb3/spotify"
)

type App struct {
	client *spotify.Client
	config config.Config
}

var (
	app                 = &App{}
	popularTrackIDs     = make(map[spotify.ID]bool)
	lessPopularTrackIDs = make(map[spotify.ID]bool)
	wg                  sync.WaitGroup
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	config, _ := loadConfig()
	app.config = config
	app.client = getSpotifyClient()

	consumer, err := kafkaConsumerSetup()

	if err != nil {
		log.Fatal(err)
	}

	listOfLessPopularTrackIDs, listOfPopularTrackIDs, err := getTrackListFromTopics(consumer)

	err = app.addTracksToPlaylist("Bangers", "All this heat is brought to you by Kafka", listOfPopularTrackIDs...)
	if err != nil {
		log.Fatalln("Error adding tracks to playlist: " + err.Error())
	}
	err = app.addTracksToPlaylist("Potential Bangers", "All this heat is brought to you by Kafka", listOfLessPopularTrackIDs...)
	if err != nil {
		log.Fatalln("Error adding tracks to playlist: " + err.Error())
	}
}

func kafkaConsumerSetup() (sarama.Consumer, error) {
	brokers := app.config.KafkaConfig.BootstrapServer
	fmt.Println("brokers", brokers)
	consumer, err := sarama.NewConsumer(brokers, nil)

	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func getTrackListFromTopics(consumer sarama.Consumer) ([]spotify.ID, []spotify.ID, error) {
	wg.Add(2)

	popularTracksChannel := make(chan []spotify.ID, 1)
	lessPopularTracksChannel := make(chan []spotify.ID, 1)

	defer close(popularTracksChannel)
	defer close(lessPopularTracksChannel)

	go consumeTracks(consumer, app.config.KafkaConfig.Topics.PopularTracks, popularTracksChannel, popularTrackIDs)
	go consumeTracks(consumer, app.config.KafkaConfig.Topics.LessPopularTracks, lessPopularTracksChannel, lessPopularTrackIDs)

	wg.Wait()

	listOfLessPopularTrackIDs := <-lessPopularTracksChannel
	listOfPopularTrackIDs := <-popularTracksChannel

	return listOfLessPopularTrackIDs, listOfPopularTrackIDs, nil
}

func getSpotifyClient() *spotify.Client {
	client, err := auth.Authenticate()

	if err != nil {
		log.Fatal(err)
	}

	return client
}

func consumeTracks(consumer sarama.Consumer, topic string, trackIDChannel chan []spotify.ID, consumedIDs map[spotify.ID]bool) {
	initialOffset := sarama.OffsetOldest
	maxNumSongs := app.config.MaxNumSongs
	partitionList, _ := consumer.Partitions(topic)
	timeout := app.config.KafkaConfig.ConsumerTimeout
	timer := time.NewTimer(timeout * time.Second)
	defer wg.Done()

	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, initialOffset)

		if err != nil {
			log.Fatal(err.Error())
		}

		go func(pc sarama.PartitionConsumer, partitionNum int32) {
			var results []spotify.ID
		ConsumerLoop:
			for {
				select {
				case msg := <-pc.Messages():
					var track spotify.FullTrack
					json.Unmarshal(msg.Value, &track)
					if _, exists := consumedIDs[track.ID]; !exists && len(results) < maxNumSongs {
						results = append(results, track.ID)
						consumedIDs[track.ID] = true
					}

					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(timeout * time.Second)
				case <-timer.C:
					trackIDChannel <- results
					break ConsumerLoop
				}
			}
			log.Printf("Partition #%d for topic %s status = donezoo", partitionNum, topic)

		}(pc, partition)
	}
}

func (a *App) addTracksToPlaylist(name, description string, trackIDs ...spotify.ID) error {
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

func loadConfig() (config.Config, error) {
	config := config.Config{}
	file, err := os.Open("popular-tracks-consumer/config/config.json")

	if err != nil {
		return config, fmt.Errorf("Error opening config file: %s", err.Error())
	}

	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)

	if err != nil {
		return config, fmt.Errorf("Error decoding config file: %s", err.Error())
	}

	return config, nil
}
