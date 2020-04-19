package config

type Config struct {
	KafkaConfig KafkaConfig       `json:"kafka"`
	Playlists   map[string]string `json:"playlists"`
	Limit       int               `json:"maxNumSongsToFetch"`
}

type KafkaConfig struct {
	BootstrapServer []string `json:"bootstrapServers"`
	Topics          Topics   `json:"topics"`
}

type Topics struct {
	PlaylistTracks string `json:"tracks"`
}
