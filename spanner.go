package main

import (
	"context"
	"log"

	"cloud.google.com/go/spanner"
)

func CreateClient(ctx context.Context, db string, spannerMinOpened uint64) *spanner.Client {
	ctx, span := startSpan(ctx, "createClient")
	defer span.End()

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

func CreateClientWithWarmUp(ctx context.Context, db string, spannerMinOpened uint64) (*spanner.Client, error) {
	ctx, span := startSpan(ctx, "createClientWithWarmUp")
	defer span.End()

	client := CreateClient(ctx, db, spannerMinOpened)
	if err := client.Single().Query(ctx, spanner.NewStatement("SELECT 1")).Do(func(r *spanner.Row) error {
		return nil
	}); err != nil {
		return nil, err
	}
	return client, nil
}
