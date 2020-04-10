package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/spotify-playlist-generator/track_producer/auth"
	"github.com/zmb3/spotify"
)

type App struct {
	client *spotify.Client
}

const redirectURI = "http://localhost:8080/callback"

var (
	app = &App{}
	// auth   = spotify.NewAuthenticator(redirectURI, spotify.ScopeUserReadPrivate)
	ch     = make(chan *spotify.Client)
	state  = "abc123"
	tracks = make(chan spotify.FullTrack)
)

func main() {
	// load config containing playlist IDs
	config, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	// authenticates user and stores tokens
	client, err := auth.Authenticate()

	if err != nil {
		log.Fatal(err)
	}

	app.client = client

	producer, err := KafkaProducerSetup()
	if err != nil {
		log.Fatal(err)
	}

	// get playlist tracks and produce to kafka
	playlists := config["playlists"].(map[string]interface{})
	for playlistName, playlistID := range playlists {
		go app.GetPlaylistTracks(playlistName, playlistID.(string), tracks)
	}

	for track := range tracks {
		encodedTrack, err := json.Marshal(track)
		if err != nil {
			log.Fatalf("Error marshalling track: %s", err.Error())
		}

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "playlist-tracks",
			Key:   sarama.StringEncoder(track.SimpleTrack.ID),
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
	limit := 10
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

func KafkaProducerSetup() (sarama.SyncProducer, error) {
	//setup relevant config info
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = sarama.MaxVersion
	config.Net.MaxOpenRequests = 1
	config.Producer.Idempotent = true
	bootstrapServers := []string{"localhost:9092"}

	producer, err := sarama.NewSyncProducer(bootstrapServers, config)

	if err != nil {
		return nil, fmt.Errorf("Error creating kafka producer: %s", err.Error())
	}

	return producer, nil
}

func loadConfig() (map[string]interface{}, error) {
	var config map[string]interface{}
	file, err := os.Open("track_producer/config.json")

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
