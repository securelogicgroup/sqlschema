package sqlschema

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestOpenValidFilenamesAccepted(t *testing.T) {
	f := http.Dir("./test/valid_names")
	db, err := Open("sqlite3", "file:test.db?mode=memory", f)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func TestOpenInvalidFilenamesRejected(t *testing.T) {
	f := http.Dir("./test/invalid_names")
	_, err := Open("sqlite3", "file:test.db?mode=memory", f) // Won't return a DB
	var e InvalidUpdateFilesError
	if !errors.As(err, &e) {
		t.Error("expected Open() to fail with InvalidUpdateFilesError")
	}
}

func TestOpenSubdirRejected(t *testing.T) {
	f := http.Dir("./test/include_subdir")
	_, err := Open("sqlite3", "file:test.db?mode=memory", f) // Won't return a *DB
	var e InvalidUpdateFilesError
	if !errors.As(err, &e) {
		t.Error("expected Open() to fail with InvalidUpdateFilesError")
	}
}

func TestOpenNonSequentialRejected(t *testing.T) {
	f := http.Dir("./test/invalid_sequence")
	_, err := Open("sqlite3", "file:test.db?mode=memory", f) // Won't return a *DB
	var e InvalidUpdateFilesError
	if !errors.As(err, &e) {
		t.Error("expected Open() to fail with InvalidUpdateFilesError")
	}
}

func TestOpenMustStartWithOne(t *testing.T) {
	f := http.Dir("./test/missing_one")
	_, err := Open("sqlite3", "file:test.db?mode=memory", f) // Won't return a *DB
	var e InvalidUpdateFilesError
	if !errors.As(err, &e) {
		t.Error("expected Open() to fail with InvalidUpdateFilesError")
	}
}

func TestOpenMustHaveUpdates(t *testing.T) {
	dir, err := ioutil.TempDir("", "sqlschema")
	if err != nil {
		t.Fatalf("couldn't create temp dir: %s", err)
	}
	defer os.RemoveAll(dir)
	f := http.Dir(dir)
	_, err = Open("sqlite3", "file:test.db?mode=memory", f) // Should never return a *DB
	var e InvalidUpdateFilesError
	if !errors.As(err, &e) {
		t.Error("expected Open() to fail with InvalidUpdateFilesError")
	}
}

func TestOpenValidUpdatesApplied(t *testing.T) {
	f := http.Dir("./test/valid_updates")
	db, err := Open("sqlite3", "file:test.db?mode=memory", f)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT * FROM a;`)
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
}

func TestOpenCorrectUpdatesLogged(t *testing.T) {
	f := http.Dir("./test/correct_log")
	db, err := Open("sqlite3", "file:test.db?mode=memory", f)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT filename FROM schema_updates ORDER BY seq ASC;`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	names := []string{"1.sql", "2.sql", "3.sql"}[:]
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if len(names) == 0 {
			t.Errorf("not expecting another file: %s", name)
			continue
		}
		if names[0] != name {
			t.Errorf("logged update %s doesn't match expected %s", name, names[0])
		}
		names = names[1:]
	}
	for _, name := range names {
		t.Errorf("missing update: %s", name)
	}
}

func TestOpenValidUpdatesPartiallyApplied(t *testing.T) {
	dir := http.Dir("./test/partial_valid/updates")
	tmp, err := ioutil.TempFile("", "sqlschema")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	f, err := os.Open("./test/partial_valid/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, err = io.Copy(tmp, f)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open("sqlite3", fmt.Sprintf("file:%s", tmp.Name()), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}

func TestOpenInvalidSchemaNoWrite(t *testing.T) {
	dir := http.Dir("./test/invalid_updates/updates")
	tmp, _ := ioutil.TempFile("", "sqlschema")
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	f, _ := os.Open("./test/invalid_updates/test.db")
	defer f.Close()
	_, _ = io.Copy(tmp, f)                                             // Ignore error
	_, err := Open("sqlite3", fmt.Sprintf("file:%s", tmp.Name()), dir) // *DB will be nil
	if err == nil {
		t.Fatal(fmt.Errorf("Open() should have failed"))
	}
	// Checksum to ensure DB didn't change
	oldfile, _ := os.Open("./test/invalid_updates/test.db")
	newfile, _ := os.Open(tmp.Name())
	defer oldfile.Close()
	defer newfile.Close()
	old := sha1.New()
	new := sha1.New()
	_, _ = io.Copy(old, oldfile)
	_, _ = io.Copy(new, newfile)
	if bytes.Compare(old.Sum(nil), new.Sum(nil)) != 0 {
		t.Errorf(
			"temp db (%s) has sha1 %s but original has %s",
			tmp.Name(),
			base64.StdEncoding.EncodeToString(new.Sum(nil)),
			base64.StdEncoding.EncodeToString(old.Sum(nil)),
		)
	}
}

func TestOpenGapsInSchemaLogFail(t *testing.T) {
	f := http.Dir("./test/gaps_in_log/updates")
	_, err := Open("sqlite3", "file:test/gaps_in_log/test.db", f)
	var e UpdateSchemaError
	if !errors.As(err, &e) {
		t.Errorf("expected Open() to fail with UpdateSchemaError")
	}
}

func TestOpenSchemaUpdatesEntriesCorrect(t *testing.T) {
	f := http.Dir("./test/correct_entries")
	db, err := Open("sqlite3", "file:test.db?mode=memory", f)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, _ := db.Query(`SELECT sha1, seq FROM schema_updates ORDER BY seq`)
	defer rows.Close()
	sha1s := []string{
		"b3df748a0fb1f665c06d7e009b947d53ac65650a",
		"c44885400f978e6ec6cf99c88adf47cb4ba3c3db",
		"ee9f6115679b5159df7d620d78a17293942021df",
	}
	seqs := []uint64{1, 2, 3}
	for rows.Next() {
		var sha1 string
		var seq uint64
		_ = rows.Scan(&sha1, &seq)
		if sha1 != sha1s[0] || seq != seqs[0] {
			t.Errorf("invalid log %d: %s (wanted %d: %s", seq, sha1, seqs[0], sha1s[0])
		}
		sha1s = sha1s[1:]
		seqs = seqs[1:]
	}
	if len(seqs) != 0 || len(sha1s) != 0 {
		t.Errorf("expected log entries remaining")
	}
}

func TestOpenSchemaUpdatesEntriesInvalidChecksumFails(t *testing.T) {
	f := http.Dir("./test/invalid_checksum/updates")
	_, err := Open("sqlite3", "file:test/invalid_checksum/test.db", f)
	var e UpdateSchemaError
	if !errors.As(err, &e) {
		t.Errorf("expected Open() to fail with UpdateSchemaError")
	}
}

func TestSchemaReapplyWithMultipleFiles(t *testing.T) {
	f := http.Dir("./test/correct_entries")
	db, err := Open("sqlite3", "file:test.db?mode=memory", f)
	if err != nil {
		t.Errorf("expected Open() to succeed but failed with error: %v", err)

	}
	defer db.Close()

	if err = Apply(db, f); err != nil {
		t.Errorf("expected Apply() to succeed but failed with error: %v", err)
	}

}
