package sqlschema

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

func updates(dir http.FileSystem) ([]update, error) {
	files, err := dir.Open("/")
	if err != nil {
		return nil, fmt.Errorf("open updates: %w", err)
	}

	list, err := files.Readdir(0)
	if err != nil {
		return nil, fmt.Errorf("listing updates: %w", err)
	}

	if len(list) < 1 {
		return nil, InvalidUpdateFilesError(fmt.Errorf("no update files in given directory"))
	}

	// Ensure no subdirectories exist and names are valid
	for _, file := range list {
		if file.IsDir() {
			return nil, InvalidUpdateFilesError(
				fmt.Errorf("updates should only contain files: %s is a directory", file.Name()),
			)
		}
		if !updateFileMask.MatchString(file.Name()) {
			return nil, InvalidUpdateFilesError(
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
				return nil, InvalidUpdateFilesError(
					fmt.Errorf("first update file (%s) should match /^0*1.sql$/", file.Name()),
				)
			}
		} else {
			prev, _ := strconv.ParseUint(strings.TrimSuffix(list[i-1].Name(), ".sql"), 10, 64)
			if this-prev != 1 {
				return nil, InvalidUpdateFilesError(
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
		f, err := dir.Open(up.filename)
		if err != nil {
			return nil, fmt.Errorf("opening %s: %w", up.filename, err)
		}
		b, err := ioutil.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", up.filename, err)
		}
		up.contents = string(b)
		s := sha1.Sum(b)
		up.sha1 = hex.EncodeToString(s[:])
		available = append(available, up)
	}

	return available, nil
}
