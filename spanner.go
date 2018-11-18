package main

import (
	"context"
	"log"

	"cloud.google.com/go/spanner"
)

func CreateClient(ctx context.Context, db string) *spanner.Client {
	o := spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened: 1,
		},
	}
	dataClient, err := spanner.NewClientWithConfig(ctx, db, o)
	if err != nil {
		log.Fatal(err)
	}

	return dataClient
}
