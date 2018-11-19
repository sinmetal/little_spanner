package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

type Tweet struct {
	ID         string `spanner:"Id"`
	CreatedAt  time.Time
	CommitedAt time.Time
}

type TweetStore struct {
	sc *spanner.Client
}

func (s *TweetStore) Insert(ctx context.Context, id string) error {
	ctx, span := startSpan(ctx, "insert")
	defer span.End()

	now := time.Now()

	ml := []*spanner.Mutation{}
	for i := 0; i < 3; i++ {
		t := Tweet{
			ID:         id,
			CreatedAt:  now,
			CommitedAt: spanner.CommitTimestamp,
		}
		m, err := spanner.InsertStruct(fmt.Sprintf("Tweet%d", i), t)
		if err != nil {
			return err
		}
		ml = append(ml, m)
	}

	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite(ml)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *TweetStore) UpdateSamplingRow(ctx context.Context) error {
	ctx, span := startSpan(ctx, "updateSamplingRow")
	defer span.End()

	sql := `SELECT * FROM Tweet0 TABLESAMPLE RESERVOIR (1 ROWS);`
	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		iter := txn.Query(ctx, spanner.Statement{SQL: sql})
		defer iter.Stop()

		t := Tweet{}
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}
			if err := row.ToStruct(&t); err != nil {
				return err
			}
		}
		m, err := spanner.UpdateStruct("Tweet0", &t)
		if err != nil {
			return err
		}
		return txn.BufferWrite([]*spanner.Mutation{m})
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *TweetStore) Grand(ctx context.Context) error {
	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		id := uuid.New().String()

		if err := s.Insert(ctx, id); err != nil {
			return err
		}
		if err := s.UpdateSamplingRow(ctx); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
