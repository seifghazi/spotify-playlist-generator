package main

import (
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

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	code := r.URL.Query().Get("code")
	if code != "" {
		a.code = code
		fmt.Fprintln(w, "Login is successful, You may close the browser and goto commandline")
	} else {
		fmt.Fprintln(w, "Login is not successful, You may close the browser and try again")
	}
	a.shutdownSignal <- "shutdown"
}

// redirectURI is the OAuth redirect URI for the application.
// You must register an application at Spotify's developer portal
// and enter this value.
const redirectURI = "http://localhost:8080/callback"

var (
	auth  = spotify.NewAuthenticator(redirectURI, spotify.ScopeUserReadPrivate)
	ch    = make(chan *spotify.Client)
	state = "abc123"
)

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

func main() {

	// first start an HTTP server
	http.HandleFunc("/callback", completeAuth)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Got request for:", r.URL.String())
	})
	go http.ListenAndServe(":8080", nil)

	url := auth.AuthURL(state)
	fmt.Println("Please log in to Spotify by visiting the following page in your browser:", url)

	// wait for auth to complete
	client := <-ch

	app := App{
		client: client,
	}
	// use the client to make calls that require authorization
	user, err := client.CurrentUser()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("You are logged in as:", user.ID)
	playlists := map[string]string{
		"Global Top 50":   "37i9dQZEVXbMDoHDwVN2tF",
		"Chill Hits":      "37i9dQZF1DX4WYpdgoIcn6",
		"Hip Hop Central": "37i9dQZF1DWY6tYEFs22tT",
	}

	// bootstrapServers := []string{"localhost:9092"}
	//setup relevant config info
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = sarama.MaxVersion
	config.Net.MaxOpenRequests = 1
	config.Producer.Idempotent = true
	// producer, err := sarama.NewSyncProducer(bootstrapServers, config)

	tracks := make(chan *spotify.PlaylistTrackPage)
	for playlistName, playlistID := range playlists {
		go app.GetPlaylistTracks(playlistName, playlistID, tracks)
	}

	for response := range tracks {

		fmt.Println(response.Tracks[0].Track.Popularity)

		// if err != nil {
		// 	fmt.Println(err.Error())
		// 	return
		// }

		// _, _, err = producer.SendMessage(&sarama.ProducerMessage{
		// 	Topic: "playlist-tracks",
		// 	Value: sarama.StringEncoder(response),
		// })

		// if err != nil {
		// 	fmt.Println(err.Error())
		// }
	}

}

func (a *App) GetPlaylistTracks(playlistName, playlistID string, tracks chan *spotify.PlaylistTrackPage) {
	fmt.Println("Fetching tracks for playlist: " + playlistName)
	// send request
	limit := 10
	playlist, err := a.client.GetPlaylistTracksOpt(spotify.ID(playlistID), &spotify.Options{Limit: &limit}, "items(name, track(name,album(name, id),artists,id, popularity))")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	tracks <- playlist

}

func KafkaSetup() {

}
