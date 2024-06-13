package migrate

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrate(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	t.Run("applies migrations", func(t *testing.T) {
		migrations := map[int]Migration{
			1: func(tx *sql.Tx) error {
				_, err := tx.Exec("create table users (id integer primary key)")
				return err
			},
			2: func(tx *sql.Tx) error {
				_, err := tx.Exec("create table posts (id integer primary key)")
				return err
			},
		}
		err := Migrate(db, migrations)
		require.NoError(t, err)

		_, err = db.Exec("select * from users")
		require.NoError(t, err)

		_, err = db.Exec("select * from posts")
		require.NoError(t, err)
	})

	t.Run("applies only new migrations", func(t *testing.T) {
		migrations := map[int]Migration{
			1: func(tx *sql.Tx) error {
				_, err := tx.Exec("create table users (id integer primary key)")
				return err
			},
		}
		err := Migrate(db, migrations)
		require.NoError(t, err)

		migrations = map[int]Migration{
			1: func(tx *sql.Tx) error {
				_, err := tx.Exec("create table users (id integer primary key)")
				return err
			},
			2: func(tx *sql.Tx) error {
				_, err := tx.Exec("create table posts (id integer primary key)")
				return err
			},
		}
		err = Migrate(db, migrations)
		require.NoError(t, err)
	})

	t.Run("rolls back to last successful migration", func(t *testing.T) {
		migrations := map[int]Migration{
			1: func(tx *sql.Tx) error {
				_, err := tx.Exec("create table users (id integer primary key)")
				return err
			},
			2: func(tx *sql.Tx) error {
				_, err := tx.Exec("create table posts (id integer primary key)")
				return err
			},
			3: func(tx *sql.Tx) error {
				return fmt.Errorf("failed to apply migration 3")
			},
			4: func(tx *sql.Tx) error {
				return fmt.Errorf("failed to apply migration 4")
			},
		}
		err := Migrate(db, migrations)
		require.Error(t, err)

		_, err = db.Exec("select * from users")
		require.NoError(t, err)

		_, err = db.Exec("select * from posts")
		require.NoError(t, err)
	})

}
