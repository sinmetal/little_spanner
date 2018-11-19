package main

import (
	"context"
	"os"
	"sync"
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

	id := uuid.New().String()
	if err := ts.Grand(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func TestTweetStore_NotFoundInsert(t *testing.T) {
	spannerDatabase := os.Getenv("SPANNER_DATABASE")

	ctx := context.Background()
	sc := CreateClient(ctx, spannerDatabase, 1)
	ts := TweetStore{
		sc: sc,
	}

	if err := ts.NotFoundInsert(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTweetStore_ReadIndexWithInsertHeavy(t *testing.T) {
	spannerDatabase := os.Getenv("SPANNER_DATABASE")

	ctx := context.Background()
	sc := CreateClient(ctx, spannerDatabase, 1)
	ts := TweetStore{
		sc: sc,
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 1000000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := ts.ReadIndexWithInsertHeavy(ctx); err != nil {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()

}
