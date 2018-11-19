package main

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
)

func TestTweetStore_Insert(t *testing.T) {
	spannerDatabase := os.Getenv("SPANNER_DATABASE")

	ctx := context.Background()
	sc := CreateClient(ctx, spannerDatabase, 1)
	ts := TweetStore{
		sc: sc,
	}

	id := uuid.New().String()
	if err := ts.Insert(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func TestTweetStore_Grand(t *testing.T) {
	spannerDatabase := os.Getenv("SPANNER_DATABASE")

	ctx := context.Background()
	sc := CreateClient(ctx, spannerDatabase, 1)
	ts := TweetStore{
		sc: sc,
	}

	if err := ts.Grand(ctx); err != nil {
		t.Fatal(err)
	}
}
