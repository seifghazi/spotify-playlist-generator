package config

import "time"

type Config struct {
	RedirectURI string      `json:"redirectURI"`
	KafkaConfig KafkaConfig `json:"kafka"`
	MaxNumSongs int         `json:"maxNumSongs"`
}

type KafkaConfig struct {
	BootstrapServer []string      `json:"bootstrapServers"`
	Topics          Topics        `json:"topics"`
	ConsumerTimeout time.Duration `json:"timeoutInSecs"`
}

type Topics struct {
	PopularTracks     string `json:"popularTracks"`
	LessPopularTracks string `json:"lessPopularTracks"`
}
