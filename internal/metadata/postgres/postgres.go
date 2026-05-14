package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/kevrocks67/latent/internal/metadata"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) GetRecord(ctx context.Context, key string) (*metadata.Record, error) {
	query := `
		SELECT
			cache_key, object_key, owner_node, state, etag,
			size_bytes, fresh_until, validated_at, created_at, updated_at
		FROM cache_records
		WHERE cache_key = $1`

	var r metadata.Record
	err := s.db.QueryRowContext(ctx, query, key).Scan(
		&r.CacheKey,
		&r.ObjectKey,
		&r.OwnerNode,
		&r.State,
		&r.ETag,
		&r.SizeBytes,
		&r.FreshUntil,
		&r.ValidatedAt,
		&r.CreatedAt,
		&r.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("No matching rows found for key %s: %w", key, err)
	}

	if err != nil {
		return nil, fmt.Errorf("postgres SELECT error: %w", err)
	}

	return &r, nil
}

func (s *PostgresStore) UpsertRecord(ctx context.Context, record *metadata.Record) error {
	query := `
		INSERT INTO cache_records (
			cache_key, object_key, owner_node, state, etag,
			size_bytes, fresh_until, validated_at, created_at, updated_at
		)
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (cache_key) DO UPDATE SET
			owner_node = EXCLUDED.owner_node,
			state = EXCLUDED.state,
			etag = EXCLUDED.etag,
			size_bytes = EXCLUDED.size_bytes,
			fresh_until = EXCLUDED.fresh_until,
			validated_at = EXCLUDED.validated_at,
			updated_at = NOW()`

	_, err := s.db.ExecContext(ctx, query,
		record.CacheKey,    // $1
		record.ObjectKey,   // $2
		record.OwnerNode,   // $3
		record.State,       // $4
		record.ETag,        // $5
		record.SizeBytes,   // $6
		record.FreshUntil,  // $7
		record.ValidatedAt, // $8
		record.CreatedAt,   // $9
		record.UpdatedAt,   // $10
	)
	if err != nil {
		return fmt.Errorf("postgres upsert error: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpdateState(ctx context.Context, key string, state metadata.CacheState) error {
	query := `UPDATE cache_records SET state = $1, updated_at = NOW() WHERE cache_key = $2`
	_, err := s.db.ExecContext(ctx, query, state, key)
	if err != nil {
		return fmt.Errorf("postgres update state error: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpdateSizeBytes(ctx context.Context, key string, size int64) error {
	return nil
}

func (s *PostgresStore) SetReady(ctx context.Context, key string, size int64, etag string) error {
	query := `
		UPDATE cache_records
		SET state = 'ready',
		    size_bytes = $1,
		    etag = $2,
		    validated_at = NOW(),
		    updated_at = NOW()
		WHERE cache_key = $3`
	_, err := s.db.ExecContext(ctx, query, size, etag, key)
	if err != nil {
		return fmt.Errorf("postgres set ready error: %w", err)
	}
	return nil
}

func (s *PostgresStore) DeleteRecord(ctx context.Context, key string) error {
	query := `DELETE FROM cache_records WHERE cache_key = $1`

	_, err := s.db.ExecContext(ctx, query, key)
	if err != nil {
		return fmt.Errorf("postgres delete error: %w", err)
	}
	return nil
}
