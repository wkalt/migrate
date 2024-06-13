package migrate

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
)

var (
	errMissingSchemaMigration = errors.New("missing schema migrations")
)

// Migration represents a database migration.
type Migration func(tx *sql.Tx) error

func isUndefinedTable(err error) (bool, error) {
	var pqerr = &pq.Error{}
	var sqliteErr sqlite3.Error

	switch {
	case errors.As(err, &pqerr):
		return pqerr.Code.Name() == "undefined_table", nil
	case errors.As(err, &sqliteErr):
		return err.Error() == "no such table: schema_migrations", nil
	default:
		return false, fmt.Errorf("unsupported driver")
	}
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
			undefined, err2 := isUndefinedTable(err)
			if err2 != nil {
				return fmt.Errorf("failed to parse error: %w", err2)
			}
			if undefined {
				return errMissingSchemaMigration
			}
			return fmt.Errorf("failed to select max applied migration: %w", err)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errMissingSchemaMigration) {
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
			_, err = tx.Exec(`insert into schema_migrations (version, created_at)
			values ($1, $2)`, k, time.Now().Format(time.RFC3339))
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

func initializeSchemaMigrations(db *sql.DB) error {
	if _, err := db.Exec(`
	create table schema_migrations (
		version int primary key,
		created_at text not null
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
