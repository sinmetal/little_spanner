package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
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

func (s *TweetStore) NotFoundInsert(ctx context.Context) error {
	ctx, span := startSpan(ctx, "notFoundInsert")
	defer span.End()

	id := uuid.New().String()
	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		ml := []*spanner.Mutation{}
		row, err := txn.ReadRow(ctx, "Tweet0", spanner.Key{id}, []string{"Id"})
		if err != nil {
			if spanner.ErrCode(err) == codes.NotFound {
				fmt.Printf("%s is not found Tweet0\n", id)
				m, err := spanner.InsertStruct("Tweet0", Tweet{
					ID:         id,
					CreatedAt:  time.Now(),
					CommitedAt: spanner.CommitTimestamp,
				})
				if err != nil {
					return err
				}
				ml = append(ml, m)
			} else {
				return err
			}
		} else {
			t := Tweet{}
			if err := row.ToStruct(&t); err != nil {
				return err
			}

			m, err := spanner.UpdateStruct("Tweet0", &t)
			if err != nil {
				return err
			}
			ml = append(ml, m)
		}

		return txn.BufferWrite(ml)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *TweetStore) Grand(ctx context.Context, id string) error {
	ctx, span := startSpan(ctx, "grand")
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
		{
			sql := `SELECT * FROM Tweet0 TABLESAMPLE RESERVOIR (1 ROWS);`

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
			ml = append(ml, m)
		}
		{
			notFoundID := uuid.New().String()
			row, err := txn.ReadRow(ctx, "Tweet0", spanner.Key{notFoundID}, []string{"Id"})
			if err != nil {
				if spanner.ErrCode(err) == codes.NotFound {
					fmt.Printf("%s is not found Tweet0\n", notFoundID)
					m, err := spanner.InsertStruct("Tweet0", Tweet{
						ID:         id,
						CreatedAt:  time.Now(),
						CommitedAt: spanner.CommitTimestamp,
					})
					if err != nil {
						return err
					}
					ml = append(ml, m)
				} else {
					return err
				}
			} else {
				t := Tweet{}
				if err := row.ToStruct(&t); err != nil {
					return err
				}

				m, err := spanner.UpdateStruct("Tweet0", &t)
				if err != nil {
					return err
				}
				ml = append(ml, m)
			}
		}
		return txn.BufferWrite(ml)
	})
	if err != nil {
		return err
	}
	return nil
}
