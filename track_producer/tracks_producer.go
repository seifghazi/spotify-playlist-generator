package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/zmb3/spotify"
)

type App struct {
	client         *spotify.Client
	shutdownSignal chan string
	server         *http.Server
	code           string
}

const redirectURI = "http://localhost:8080/callback"

var (
	app    = &App{}
	auth   = spotify.NewAuthenticator(redirectURI, spotify.ScopeUserReadPrivate)
	ch     = make(chan *spotify.Client)
	state  = "abc123"
	tracks = make(chan *spotify.PlaylistTrackPage)
)

func main() {
	Authenticate()

	producer, err := KafkaProducerSetup()

	if err != nil {
		fmt.Println(err)
		return
	}

	playlists := map[string]string{
		"Global Top 50":   "37i9dQZEVXbMDoHDwVN2tF",
		"Chill Hits":      "37i9dQZF1DX4WYpdgoIcn6",
		"Hip Hop Central": "37i9dQZF1DWY6tYEFs22tT",
	}
	for playlistName, playlistID := range playlists {
		go app.GetPlaylistTracks(playlistName, playlistID, tracks)
	}

	for trackList := range tracks {
		parsedTrackList, err := json.Marshal(trackList)
		fmt.Println(string(parsedTrackList))

		if err != nil {
			fmt.Println(err.Error())
			return
		}

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "playlist-tracks",
			Value: sarama.StringEncoder(string(parsedTrackList)),
		})

		if err != nil {
			fmt.Println(err.Error())
		}
	}
}

func Authenticate() {
	http.HandleFunc("/callback", completeAuth)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Got request for:", r.URL.String())
	})
	go http.ListenAndServe(":8080", nil)

	url := auth.AuthURL(state)
	fmt.Println("Please log in to Spotify by visiting the following page in your browser:", url)

	// wait for auth to complete
	client := <-ch
	app.client = client

	// use the client to make calls that require authorization
	user, err := client.CurrentUser()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("You are logged in as:", user.ID)
}

// GetPlaylistTracks gets tracks belonging to a playlist
func (a *App) GetPlaylistTracks(playlistName, playlistID string, tracks chan *spotify.PlaylistTrackPage) {
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

	tracks <- playlistTracks
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

func completeAuth(w http.ResponseWriter, r *http.Request) {
	tok, err := auth.Token(state, r)
	if err != nil {
		http.Error(w, "Couldn't get token", http.StatusForbidden)
		log.Fatal(err)
	}
	if st := r.FormValue("state"); st != state {
		http.NotFound(w, r)
		log.Fatalf("State mismatch: %s != %s\n", st, state)
	}
	// use the token to get an authenticated client
	client := auth.NewClient(tok)
	fmt.Fprintf(w, "Login Completed!")
	ch <- &client
}
