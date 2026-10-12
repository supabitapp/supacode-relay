package push

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

var ErrConflict = errors.New("registration conflict")
var ErrUnavailable = errors.New("recipient unavailable")
var ErrCapacity = errors.New("gateway capacity reached")
var ErrRateLimited = errors.New("enrollment rate limited")

type Recipient struct {
	ID              string
	Host            string
	Generation      int64
	Token           string
	Topic           string
	Environment     string
	Status          string
	ChallengeHash   string
	ChallengeExpiry int64
	RevocationHash  string
	UpdatedAt       int64
}

type Store struct{ db *sql.DB }

func OpenStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = file.Chmod(0600); err != nil {
		file.Close()
		return nil, err
	}
	file.Close()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db}
	_, err = db.Exec(`
		PRAGMA busy_timeout = 5000;
		PRAGMA journal_mode = WAL;
		CREATE TABLE IF NOT EXISTS recipients (
		 id TEXT PRIMARY KEY, host TEXT NOT NULL, generation INTEGER NOT NULL,
		 token TEXT NOT NULL, topic TEXT NOT NULL, environment TEXT NOT NULL, status TEXT NOT NULL,
		 challenge_hash TEXT NOT NULL, challenge_expiry INTEGER NOT NULL,
		 revocation_hash TEXT NOT NULL, updated_at INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS recipients_host ON recipients(host);
 CREATE INDEX IF NOT EXISTS recipients_status_updated ON recipients(status,updated_at);
		CREATE TABLE IF NOT EXISTS receipts (
		 recipient_id TEXT NOT NULL, generation INTEGER NOT NULL, notification_id TEXT NOT NULL,
		 digest TEXT NOT NULL, status TEXT NOT NULL, invalid_token INTEGER NOT NULL DEFAULT 0,
		 lease_until INTEGER NOT NULL, expires_at INTEGER NOT NULL, claim_token TEXT NOT NULL,
		 PRIMARY KEY(recipient_id, generation, notification_id)
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS receipts_expiry ON receipts(expires_at);
		CREATE TABLE IF NOT EXISTS enrollment_limits (token_hash TEXT PRIMARY KEY, last_sent INTEGER NOT NULL);
	`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (store *Store) Close() error { return store.db.Close() }

func hash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func scanRecipient(row *sql.Row) (Recipient, error) {
	var recipient Recipient
	err := row.Scan(&recipient.ID, &recipient.Host, &recipient.Generation, &recipient.Token, &recipient.Topic,
		&recipient.Environment, &recipient.Status, &recipient.ChallengeHash, &recipient.ChallengeExpiry,
		&recipient.RevocationHash, &recipient.UpdatedAt)
	return recipient, err
}

func (store *Store) Recipient(ctx context.Context, id string) (Recipient, error) {
	return scanRecipient(store.db.QueryRowContext(ctx, `SELECT * FROM recipients WHERE id = ?`, id))
}

func (store *Store) Enroll(ctx context.Context, recipient Recipient, now time.Time) (Recipient, bool, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Recipient{}, false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM recipients WHERE (status = 'pending' AND updated_at < ?) OR (status = 'revoked' AND updated_at < ?)`, now.Add(-24*time.Hour).UnixMilli(), now.Add(-30*24*time.Hour).UnixMilli()); err != nil {
		return Recipient{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM enrollment_limits WHERE last_sent < ?`, now.Add(-24*time.Hour).UnixMilli()); err != nil {
		return Recipient{}, false, err
	}
	prior, err := scanRecipient(tx.QueryRowContext(ctx, `SELECT * FROM recipients WHERE id = ?`, recipient.ID))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Recipient{}, false, err
	}
	if err == nil {
		if prior.Host != recipient.Host || prior.Generation > recipient.Generation {
			return Recipient{}, false, ErrConflict
		}
		if prior.Generation == recipient.Generation {
			if prior.Token != recipient.Token || prior.Topic != recipient.Topic || prior.Environment != recipient.Environment || prior.RevocationHash != recipient.RevocationHash || prior.Status == "revoked" {
				return Recipient{}, false, ErrConflict
			}
			if prior.Status == "active" {
				return prior, false, nil
			}
		}
	} else {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM recipients WHERE host = ?`, recipient.Host).Scan(&count); err != nil {
			return Recipient{}, false, err
		}
		if count >= 64 {
			return Recipient{}, false, ErrConflict
		}
		var total, pending int
		if err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM recipients), (SELECT COUNT(*) FROM recipients WHERE status = 'pending')`).Scan(&total, &pending); err != nil {
			return Recipient{}, false, err
		}
		if total >= 100000 || pending >= 10000 {
			return Recipient{}, false, ErrCapacity
		}
	}
	var lastSent int64
	err = tx.QueryRowContext(ctx, `SELECT last_sent FROM enrollment_limits WHERE token_hash = ?`, hash(recipient.Token)).Scan(&lastSent)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Recipient{}, false, err
	}
	if lastSent > now.Add(-time.Minute).UnixMilli() {
		return Recipient{}, false, ErrRateLimited
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO enrollment_limits VALUES (?, ?) ON CONFLICT(token_hash) DO UPDATE SET last_sent = excluded.last_sent`, hash(recipient.Token), now.UnixMilli())
	if err != nil {
		return Recipient{}, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO recipients VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)
	 ON CONFLICT(id) DO UPDATE SET generation = excluded.generation, token = excluded.token, topic = excluded.topic,
	 environment = excluded.environment, status = 'pending', challenge_hash = excluded.challenge_hash,
	 challenge_expiry = excluded.challenge_expiry, revocation_hash = excluded.revocation_hash, updated_at = excluded.updated_at`,
		recipient.ID, recipient.Host, recipient.Generation, recipient.Token, recipient.Topic, recipient.Environment,
		recipient.ChallengeHash, recipient.ChallengeExpiry, recipient.RevocationHash, now.UnixMilli())
	if err != nil {
		return Recipient{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Recipient{}, false, err
	}
	recipient.Status, recipient.UpdatedAt = "pending", now.UnixMilli()
	return recipient, true, nil
}

func (store *Store) Confirm(ctx context.Context, host, id string, generation int64, challenge string, now time.Time) error {
	result, err := store.db.ExecContext(ctx, `UPDATE recipients SET status = 'active', challenge_hash = ''
	 WHERE id = ? AND host = ? AND generation = ? AND status = 'pending' AND challenge_hash = ? AND challenge_expiry > ?`,
		id, host, generation, hash(challenge), now.UnixMilli())
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrUnavailable
	}
	return nil
}

func changed(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrUnavailable
	}
	return nil
}
func (store *Store) Revoke(ctx context.Context, host, id string, generation int64) error {
	result, err := store.db.ExecContext(ctx, `UPDATE recipients SET status = 'revoked', token = '', challenge_hash = '', updated_at = ? WHERE id = ? AND host = ? AND generation = ?`, time.Now().UnixMilli(), id, host, generation)
	return changed(result, err)
}
func (store *Store) RevokeDevice(ctx context.Context, id string, generation int64, secret string) error {
	result, err := store.db.ExecContext(ctx, `UPDATE recipients SET status = 'revoked', token = '', challenge_hash = '', updated_at = ? WHERE id = ? AND generation >= ? AND revocation_hash = ?`, time.Now().UnixMilli(), id, generation, hash(secret))
	if err := changed(result, err); err != nil {
		if !errors.Is(err, ErrUnavailable) {
			return err
		}
		_, readErr := store.Recipient(ctx, id)
		if errors.Is(readErr, sql.ErrNoRows) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
		return ErrUnavailable
	}
	return nil
}

func (store *Store) Claim(ctx context.Context, id string, generation int64, notificationID, digest string, expiry, now time.Time) (string, bool, string, error) {
	_, err := store.db.ExecContext(ctx, `DELETE FROM receipts WHERE expires_at < ?`, now.UnixMilli())
	if err != nil {
		return "", false, "", err
	}
	_, err = store.db.ExecContext(ctx, `DELETE FROM enrollment_limits WHERE last_sent < ?`, now.Add(-24*time.Hour).UnixMilli())
	if err != nil {
		return "", false, "", err
	}
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", false, "", err
	}
	claimToken := base64.RawURLEncoding.EncodeToString(bytes[:])
	var claimed string
	err = store.db.QueryRowContext(ctx, `INSERT INTO receipts SELECT ?, ?, ?, ?, 'running', 0, ?, ?, ? WHERE (SELECT COUNT(*) FROM receipts) < 500000
	 ON CONFLICT(recipient_id, generation, notification_id) DO UPDATE SET status = 'running', lease_until = excluded.lease_until, claim_token = excluded.claim_token
	 WHERE receipts.digest = excluded.digest AND receipts.status = 'retry' AND receipts.lease_until <= ?
	 RETURNING status`, id, generation, notificationID, digest, now.Add(15*time.Second).UnixMilli(), expiry.Add(5*time.Minute).UnixMilli(), claimToken, now.UnixMilli()).Scan(&claimed)
	if err == nil {
		return "claimed", false, claimToken, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, "", err
	}
	var storedDigest, status string
	var invalid bool
	var lease int64
	err = store.db.QueryRowContext(ctx, `SELECT digest, status, invalid_token, lease_until FROM receipts WHERE recipient_id = ? AND generation = ? AND notification_id = ?`, id, generation, notificationID).Scan(&storedDigest, &status, &invalid, &lease)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, "", ErrCapacity
	}
	if err != nil {
		return "", false, "", err
	}
	if storedDigest != digest {
		return "", false, "", ErrConflict
	}
	if status == "running" && lease <= now.UnixMilli() {
		result, err := store.db.ExecContext(ctx, `UPDATE receipts SET lease_until = ?, claim_token = ? WHERE recipient_id = ? AND generation = ? AND notification_id = ? AND status = 'running' AND lease_until = ?`, now.Add(15*time.Second).UnixMilli(), claimToken, id, generation, notificationID, lease)
		if err != nil {
			return "", false, "", err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return "", false, "", err
		}
		if changed == 1 {
			return "claimed", false, claimToken, nil
		}
	}
	return status, invalid, "", nil
}

func (store *Store) Finish(ctx context.Context, recipient Recipient, notificationID, claimToken string, result DeliveryResult) error {
	status := "failed"
	if result.Accepted {
		status = "accepted"
	} else if result.Retryable {
		status = "retry"
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	updated, err := tx.ExecContext(ctx, `UPDATE receipts SET status = ?, invalid_token = ?, lease_until = 0 WHERE recipient_id = ? AND generation = ? AND notification_id = ? AND status = 'running' AND claim_token = ?`, status, result.InvalidToken, recipient.ID, recipient.Generation, notificationID, claimToken)
	if err := changed(updated, err); err != nil {
		return err
	}
	if result.InvalidToken {
		if _, err := tx.ExecContext(ctx, `UPDATE recipients SET status = 'revoked', token = '', challenge_hash = '', updated_at = ? WHERE id = ? AND generation = ? AND updated_at = ?`, time.Now().UnixMilli(), recipient.ID, recipient.Generation, recipient.UpdatedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (store *Store) ExpireChallenge(ctx context.Context, id string, generation int64) error {
	_, err := store.db.ExecContext(ctx, `UPDATE recipients SET challenge_expiry = 0 WHERE id = ? AND generation = ? AND status = 'pending'`, id, generation)
	return err
}
