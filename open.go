package sqlschema

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
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

// Apply will ensure the given db has had all the .sql files from updates
// applied to it. See Open.
func Apply(db DB, updates http.FileSystem) error {
	files, err := updates.Open("/")
	if err != nil {
		return fmt.Errorf("open updates: %w", err)
	}

	list, err := files.Readdir(0)
	if err != nil {
		return fmt.Errorf("listing updates: %w", err)
	}

	if len(list) < 1 {
		return InvalidUpdateFilesError(fmt.Errorf("no update files in given directory"))
	}

	// Ensure no subdirectories exist and names are valid
	for _, file := range list {
		if file.IsDir() {
			return InvalidUpdateFilesError(
				fmt.Errorf("updates should only contain files: %s is a directory", file.Name()),
			)
		}
		if !updateFileMask.MatchString(file.Name()) {
			return InvalidUpdateFilesError(
				fmt.Errorf("file %s doesn't match regex %s", file.Name(), updateFileMask),
			)
		}
	}

	// Sort the files numerically
	sort.Slice(list, func(i, j int) bool {
		iNum, _ := strconv.ParseUint(strings.TrimSuffix(list[i].Name(), ".sql"), 10, 64)
		jNum, _ := strconv.ParseUint(strings.TrimSuffix(list[j].Name(), ".sql"), 10, 64)
		return iNum < jNum
	})

	// Ensure sequence beginning at 1, with no gaps
	for i, file := range list {
		this, _ := strconv.ParseUint(strings.TrimSuffix(file.Name(), ".sql"), 10, 64)
		if i == 0 {
			if this != 1 {
				return InvalidUpdateFilesError(
					fmt.Errorf("first update file (%s) should match /^0*1.sql$/", file.Name()),
				)
			}
		} else {
			prev, _ := strconv.ParseUint(strings.TrimSuffix(list[i-1].Name(), ".sql"), 10, 64)
			if this-prev != 1 {
				return InvalidUpdateFilesError(
					fmt.Errorf("update files must be in sequence (%s followed by %s)", file.Name(), list[i-1].Name()),
				)
			}
		}
	}

	// Prepare set of updates
	var available []update
	for _, file := range list {
		var up update
		up.filename = file.Name()
		up.seq, _ = strconv.ParseUint(strings.TrimSuffix(file.Name(), ".sql"), 10, 64) // Has already worked
		f, err := updates.Open(up.filename)
		if err != nil {
			return fmt.Errorf("opening %s: %w", up.filename, err)
		}
		b, err := ioutil.ReadAll(f)
		if err != nil {
			return fmt.Errorf("reading %s: %w", up.filename, err)
		}
		up.contents = string(b)
		s := sha1.Sum(b)
		up.sha1 = hex.EncodeToString(s[:])
		available = append(available, up)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // Call tx.Commit() first on success

	// Create schema_updates table if it is missing
	if _, err := tx.Exec(createSchemaUpdates); err != nil {
		return UpdateSchemaError(fmt.Errorf("create schema table: %w", err))
	}

	var applied []update
	rows, err := tx.Query(`SELECT filename, seq, sha1 FROM schema_updates ORDER BY seq ASC;`)
	if err != nil {
		return UpdateSchemaError(fmt.Errorf("check existing updates: %w", err))
	} else {
		defer rows.Close()
		// Build list of applied updates
		for rows.Next() {
			var up update
			if err := rows.Scan(&up.filename, &up.seq, &up.sha1); err != nil {
				return fmt.Errorf("reading applied updates: %w", err)
			}
			applied = append(applied, up)
		}
	}

	// Check that applied updates match available updates
	for _, up := range applied {
		if len(available) == 0 {
			return UpdateSchemaError(
				fmt.Errorf("unknown update %d already applied", up.seq),
			)
		}
		if up.seq != available[0].seq {
			return UpdateSchemaError(
				fmt.Errorf("update %d seen instead of expected %d", up.seq, available[0].seq),
			)
		}
		if up.sha1 != available[0].sha1 {
			return UpdateSchemaError(fmt.Errorf(
				"checksum of applied update %d (%s) does not match expected (%s)",
				up.seq,
				up.sha1,
				available[0].sha1,
			))
		}
		// Drop first available: it has already been applied
		available = available[1:]
	}

	// Apply each missing update
	for _, up := range available {
		if _, err := tx.Exec(string(up.contents)); err != nil {
			return UpdateSchemaError(
				fmt.Errorf("apply update %d (%s): %w", up.seq, up.filename, err),
			)
		}
		_, err = tx.Exec(insertSchemaUpdate, up.filename, up.seq, up.sha1, time.Now(), up.contents)
		if err != nil {
			return UpdateSchemaError(
				fmt.Errorf("record update %d (%s): %w", up.seq, up.filename, err),
			)
		}
	}

	if err := tx.Commit(); err != nil {
		return UpdateSchemaError(fmt.Errorf("commit updates: %w", err))
	}

	return nil
}
