package main

import (
	"flag"
	"log"
	"time"

	alfred "github.com/openmanet/go-alfred"
)

func main() {
	var socketPath string
	var dataType uint
	var version uint
	var payload string
	var watchInterval time.Duration

	flag.StringVar(&socketPath, "socket", alfred.DefaultSocketPath, "alfred UNIX socket path")
	flag.UintVar(&dataType, "type", 64, "alfred data type (0-255)")
	flag.UintVar(&version, "version", 1, "payload version (0-255)")
	flag.StringVar(&payload, "payload", "", "payload string to publish before requesting")
	flag.DurationVar(&watchInterval, "watch", 0, "if set, re-request data at this interval")
	flag.Parse()

	if dataType > 255 {
		log.Fatalf("data type must be <= 255, got %d", dataType)
	}

	if version > 255 {
		log.Fatalf("version must be <= 255, got %d", version)
	}

	client, err := alfred.NewClient(alfred.WithSocketPath(socketPath))
	if err != nil {
		log.Fatalf("failed to connect to alfred: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("client close error: %v", err)
		}
	}()

	if payload != "" {
		if err := client.Set(uint8(dataType), uint8(version), []byte(payload)); err != nil {
			log.Fatalf("failed to publish payload: %v", err)
		}
		log.Printf("published payload to type %d with version %d", dataType, version)
	}

	fetch := func() {
		records, err := client.Request(uint8(dataType))
		if err != nil {
			log.Fatalf("request failed: %v", err)
		}
		if len(records) == 0 {
			log.Printf("no records for type %d", dataType)
			return
		}
		for _, rec := range records {
			log.Printf("%s (v%d): %s", rec.Source, rec.Version, string(rec.Data))
		}
	}

	fetch()

	if watchInterval <= 0 {
		return
	}

	ticker := time.NewTicker(watchInterval)
	defer ticker.Stop()

	for range ticker.C {
		fetch()
	}
}
