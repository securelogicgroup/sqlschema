package sqlschema

import (
	"database/sql"
	"fmt"
)

// Return updates list without updates that have already been applied.
func filter(updates []update, tx *sql.Tx) ([]update, error) {

	var applied []update
	rows, err := tx.Query(`SELECT filename, seq, sha1 FROM schema_updates ORDER BY seq ASC;`)
	if err != nil {
		return nil, UpdateSchemaError(fmt.Errorf("check existing updates: %w", err))
	} else {
		defer rows.Close()
		// Build list of applied updates
		for rows.Next() {
			var up update
			if err := rows.Scan(&up.filename, &up.seq, &up.sha1); err != nil {
				return nil, fmt.Errorf("reading applied updates: %w", err)
			}
			applied = append(applied, up)
		}
	}

	remaining := make([]update, len(updates))
	for i := range updates {
		remaining[i] = updates[i]
	}

	// Check that applied updates match available updates
	for _, up := range applied {
		if len(updates) == 0 {
			return nil, UpdateSchemaError(
				fmt.Errorf("unknown update %d already applied", up.seq),
			)
		}
		if up.seq != updates[0].seq {
			return nil, UpdateSchemaError(
				fmt.Errorf("update %d seen instead of expected %d", up.seq, updates[0].seq),
			)
		}
		if up.sha1 != updates[0].sha1 {
			return nil, UpdateSchemaError(fmt.Errorf(
				"checksum of applied update %d (%s) does not match expected (%s)",
				up.seq,
				up.sha1,
				updates[0].sha1,
			))
		}
		remaining = remaining[1:]
	}

	return remaining, nil
}
