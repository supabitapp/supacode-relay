package push

import (
	"fmt"
	"os"
	"strings"
)

type Credentials struct {
	KeyID string
	Path  string
}

type Config struct {
	Address  string
	Database string
	TeamID   string
	Topics   []string
	Keys     map[string]Credentials
}

func ConfigFromEnvironment() (Config, error) {
	config := Config{
		Address: os.Getenv("PUSH_ADDR"), Database: os.Getenv("PUSH_DATABASE"), TeamID: os.Getenv("APNS_TEAM_ID"),
		Topics: strings.Split(os.Getenv("APNS_TOPICS"), ","), Keys: make(map[string]Credentials),
	}
	if config.Address == "" {
		config.Address = "127.0.0.1:8081"
	}
	if config.Database == "" || config.TeamID == "" || strings.TrimSpace(config.Topics[0]) == "" {
		return Config{}, fmt.Errorf("PUSH_DATABASE, APNS_TEAM_ID and APNS_TOPICS are required")
	}
	for _, environment := range []string{"sandbox", "production"} {
		prefix := "APNS_" + strings.ToUpper(environment)
		id, path := os.Getenv(prefix+"_KEY_ID"), os.Getenv(prefix+"_KEY_FILE")
		if id == "" && path == "" {
			continue
		}
		if id == "" || path == "" {
			return Config{}, fmt.Errorf("%s_KEY_ID and %s_KEY_FILE must be configured together", prefix, prefix)
		}
		config.Keys[environment] = Credentials{KeyID: id, Path: path}
	}
	if len(config.Keys) == 0 {
		return Config{}, fmt.Errorf("an APNs credential is required")
	}
	for index, topic := range config.Topics {
		config.Topics[index] = strings.TrimSpace(topic)
	}
	return config, nil
}
