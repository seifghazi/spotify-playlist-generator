package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/spotify-playlist-generator/tracks-producer/auth"
	"github.com/spotify-playlist-generator/tracks-producer/config"
	"github.com/zmb3/spotify"
)

type App struct {
	client *spotify.Client
	config config.Config
}

var (
	app    = &App{}
	tracks = make(chan spotify.FullTrack)
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	// load config containing playlist IDs
	config, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	app.config = config

	// authenticates user and stores tokens
	client, err := auth.Authenticate()

	if err != nil {
		log.Fatal(err)
	}

	app.client = client

	producer, err := app.KafkaProducerSetup()
	if err != nil {
		log.Fatal(err)
	}

	// get playlist tracks and produce to kafka
	playlists := config.Playlists
	for playlistName, playlistID := range playlists {
		go app.GetPlaylistTracks(playlistName, playlistID, tracks)
	}

	for track := range tracks {
		encodedTrack, err := json.Marshal(track)
		if err != nil {
			log.Fatalf("Error marshalling track: %s", err.Error())
		}
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: app.config.KafkaConfig.Topics.PlaylistTracks,
			Key:   sarama.StringEncoder(track.ID),
			Value: sarama.StringEncoder(string(encodedTrack)),
		})

		if err != nil {
			log.Fatalf("Error producing to kafka: %s", err.Error())
		}
	}

}

// GetPlaylistTracks gets tracks belonging to a playlist
func (a *App) GetPlaylistTracks(playlistName, playlistID string, tracks chan spotify.FullTrack) {
	fmt.Println("Fetching tracks for playlist: " + playlistName)
	// send request
	limit := a.config.Limit
	options := &spotify.Options{Limit: &limit}
	fields := "items(name, track(name,album(name, id),artists,id, popularity))"

	playlistTracks, err := a.client.GetPlaylistTracksOpt(spotify.ID(playlistID), options, fields)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	trackList := playlistTracks.Tracks
	for _, track := range trackList {
		tracks <- track.Track
	}
}

func (a *App) KafkaProducerSetup() (sarama.SyncProducer, error) {
	//setup relevant config info
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = sarama.MaxVersion
	config.Net.MaxOpenRequests = 1
	config.Producer.Idempotent = true
	bootstrapServers := a.config.KafkaConfig.BootstrapServer

	producer, err := sarama.NewSyncProducer(bootstrapServers, config)

	if err != nil {
		return nil, fmt.Errorf("Error creating kafka producer: %s", err.Error())
	}

	return producer, nil
}

func loadConfig() (config.Config, error) {
	config := config.Config{}
	file, err := os.Open("tracks-producer/config/config.json")

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
