package migrate

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sort"

	"github.com/lib/pq"
)

var ErrMissingSchemaMigration = errors.New("missing schema migrations")

type Migration func(tx *sql.Tx) error

func initializeSchemaMigrations(db *sql.DB) error {
	if _, err := db.Exec(`create table schema_migrations (
		version int primary key,
		created_at timestamptz not null default now()
	)`); err != nil {
		return err
	}
	return nil
}

func withTx(db *sql.DB, f func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// Migrate the database through outstanding migrations. Each migration is
// executed in a separate transaction, in the order of the numeric keys.
func Migrate(db *sql.DB, migrations map[int]Migration) error {
	keys := []int{}
	for k := range migrations {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	var maxApplied int
	err := withTx(db, func(tx *sql.Tx) error {
		err := tx.QueryRow("select coalesce(max(version), -1) from schema_migrations").Scan(&maxApplied)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			var pqerr = &pq.Error{}
			if errors.As(err, &pqerr) && pqerr.Code.Name() == "undefined_table" {
				return ErrMissingSchemaMigration
			}
			return fmt.Errorf("failed to select max applied migration: %w", err)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrMissingSchemaMigration) {
			if err = initializeSchemaMigrations(db); err != nil {
				return fmt.Errorf("failed to initialize schema migrations: %w", err)
			}
			return Migrate(db, migrations)
		}
		return err
	}

	for _, k := range keys {
		if k <= maxApplied {
			continue
		}
		err := withTx(db, func(tx *sql.Tx) error {
			err := migrations[k](tx)
			if err != nil {
				return err
			}
			_, err = tx.Exec("insert into schema_migrations (version) values ($1)", k)
			if err != nil {
				return err
			}
			log.Printf("Applied migration %d", k)
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}