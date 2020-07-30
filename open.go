package sqlschema

import (
	"database/sql"
	"fmt"
	"net/http"
	"regexp"
)

type update struct {
	filename  string
	seq       uint64
	sha1      string
	timestamp string
	contents  string
}

// DB represents the subset of sql.DB behaviour needed by this
// library.
type DB interface {
	Close() error
	Begin() (*sql.Tx, error)
}

// InvalidUpdateFilesError is returned when unexpected files exist in
// the http.Dir passed to Open or Apply.
type InvalidUpdateFilesError error

// UpdateSchemaError is returned when the given SQL files cannot be
// applied. Either the SQL statements contain errors, or the files
// conflict with previosly applied files.
type UpdateSchemaError error

const createSchemaUpdates = `
CREATE TABLE IF NOT EXISTS schema_updates (
	filename text,
	seq integer,
	sha1 text,
	timestamp text,
	contents text
);
`

const insertSchemaUpdate = `
INSERT INTO schema_updates(filename, seq, sha1, timestamp, contents)
VALUES($1, $2, $3, $4, $5);
`

var updateFileMask = regexp.MustCompile(`^[0-9]+\.sql$`)

// Open will open a new database given the driverName and
// dataSourceName, and ensure the created db has had all the .sql
// files from updates applied to it.
//
//  sql/
//    0001.sql
//    0002.sql
//    0003.sql
//
//  schema := http.Dir("./sql")
//  db, err := Open("sqlite3", ":memory:", schema)
func Open(driverName, dataSourceName string, updates http.FileSystem) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	if err := Apply(db, updates); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// Apply will ensure the given db has had all the .sql files from dir
// applied to it. See Open.
func Apply(db DB, dir http.FileSystem) error {

	available, err := updates(dir)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // Call tx.Commit() first on success

	if _, err := tx.Exec(createSchemaUpdates); err != nil {
		return UpdateSchemaError(fmt.Errorf("create schema table: %w", err))
	}

	pending, err := filter(available, tx)
	if err != nil {
		return err
	}

	if err := apply(pending, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return UpdateSchemaError(fmt.Errorf("commit updates: %w", err))
	}

	return nil
}

// ApplyUnsafe will apply the .sql files from dir in order, but will
// not check for already applied updates. See Open.
func ApplyUnsafe(db DB, dir http.FileSystem) error {
	available, err := updates(dir)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // Call tx.Commit() first on success

	if _, err := tx.Exec(createSchemaUpdates); err != nil {
		return UpdateSchemaError(fmt.Errorf("create schema table: %w", err))
	}

	if err := apply(available, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return UpdateSchemaError(fmt.Errorf("commit updates: %w", err))
	}

	return nil
}
