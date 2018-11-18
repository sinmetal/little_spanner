package main

import (
	"context"
	"log"

	"cloud.google.com/go/spanner"
)

func CreateClient(ctx context.Context, db string, spannerMinOpened uint64) *spanner.Client {
	o := spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened: spannerMinOpened,
		},
	}
	dataClient, err := spanner.NewClientWithConfig(ctx, db, o)
	if err != nil {
		log.Fatal(err)
	}

	return dataClient
}
