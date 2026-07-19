package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// ErrUserNotFound is returned when a lookup by username finds no row in app_user.
var ErrUserNotFound = errors.New("metadata: user not found")

// GetUserByUsername returns the user_id, bcrypt password_hash, roles, and
// disabled flag for the given username. Returns ErrUserNotFound (wrapped) when
// no such user exists.
func (c *PGMetadataClient) GetUserByUsername(ctx context.Context, username string) (userID, passwordHash string, roles []string, disabled bool, err error) {
	row := c.db.QueryRowContext(ctx,
		`SELECT user_id, password_hash, roles, disabled FROM app_user WHERE username = $1`,
		username,
	)
	if err := row.Scan(&userID, &passwordHash, pq.Array(&roles), &disabled); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", nil, false, fmt.Errorf("%w: %q", ErrUserNotFound, username)
		}
		return "", "", nil, false, fmt.Errorf("metadata: get user %q: %w", username, err)
	}
	return userID, passwordHash, roles, disabled, nil
}

// CountUsers returns the number of rows in app_user.
func (c *PGMetadataClient) CountUsers(ctx context.Context) (int, error) {
	var n int
	if err := c.db.QueryRowContext(ctx, `SELECT count(*) FROM app_user`).Scan(&n); err != nil {
		return 0, fmt.Errorf("metadata: count users: %w", err)
	}
	return n, nil
}

// CreateUser inserts a new row into app_user with a generated UUID. The
// passwordHash must already be bcrypt-hashed by the caller. A duplicate
// username violates the UNIQUE constraint and is returned as an error.
func (c *PGMetadataClient) CreateUser(ctx context.Context, username, passwordHash string, roles []string) error {
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO app_user (user_id, username, password_hash, roles) VALUES ($1, $2, $3, $4)`,
		uuid.New().String(), username, passwordHash, pq.Array(roles),
	); err != nil {
		return fmt.Errorf("metadata: create user %q: %w", username, err)
	}
	return nil
}
