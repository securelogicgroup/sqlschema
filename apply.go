package sqlschema

import (
	"database/sql"
	"fmt"
	"time"
)

func apply(updates []update, tx *sql.Tx) error {
	for _, up := range updates {
		if _, err := tx.Exec(string(up.contents)); err != nil {
			return UpdateSchemaError(
				fmt.Errorf("apply update %d (%s): %w", up.seq, up.filename, err),
			)
		}
		if _, err := tx.Exec(
			insertSchemaUpdate, up.filename, up.seq, up.sha1, time.Now(), up.contents,
		); err != nil {
			return UpdateSchemaError(
				fmt.Errorf("record update %d (%s): %w", up.seq, up.filename, err),
			)
		}
	}
	return nil
}
