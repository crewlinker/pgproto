package pgproto_test

import (
	"embed"
	"path/filepath"
	"testing"

	"github.com/crewlinker/pgproto"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/*.sql
var testdata embed.FS

func TestParse(t *testing.T) {
	for _, tt := range []struct{ filename string }{
		{filename: "simple_select.sql"},
		{filename: "simple_insert.sql"},
		{filename: "simple_delete.sql"},
		{filename: "simple_update.sql"},
	} {
		t.Run(tt.filename, func(t *testing.T) {
			data, err := testdata.ReadFile(filepath.Join("testdata", tt.filename))
			require.NoError(t, err)

			actions, err := pgproto.ParseFullTyped(data)
			require.NoError(t, err)
			require.Len(t, actions, 1)
		})
	}
}

func TestUnsupportedStatm(t *testing.T) {
	data, err := testdata.ReadFile(filepath.Join("testdata", "unsupported_stmt.sql"))
	require.NoError(t, err)

	_, err = pgproto.ParseFullTyped(data)
	require.ErrorContains(t, err, "only support")
}
