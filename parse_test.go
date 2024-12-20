package pgproto_test

import (
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/crewlinker/pgproto"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/*
var testdata embed.FS

func TestParse(t *testing.T) {
	for _, tt := range []struct{ filename string }{
		{filename: "simple_select.sql"},
		{filename: "simple_insert.sql"},
		{filename: "simple_delete.sql"},
		{filename: "simple_update.sql"},
		{filename: "double_cast_select.sql"},
		{filename: "multi_cast_insert.sql"},
		{filename: "constructor_cast_select.sql"},
	} {
		t.Run(tt.filename, func(t *testing.T) {
			data, err := testdata.ReadFile(filepath.Join("testdata", tt.filename))
			require.NoError(t, err)

			actions, err := pgproto.ParseFullTyped(data)
			require.NoError(t, err)
			require.Len(t, actions, 1)

			actJSON, err := json.MarshalIndent(actions, "", "  ")
			require.NoError(t, err)

			expData, err := testdata.ReadFile(filepath.Join("testdata", tt.filename+".json"))
			if os.IsNotExist(err) && os.Getenv("PGPROTO_REFRESH_SNAPSHOT") != "" {
				fmt.Fprintf(os.Stderr, "refreshed snapshot for: %s", tt.filename)

				os.WriteFile(filepath.Join("testdata", tt.filename+".json"), actJSON, 0o777)
				expData = actJSON
			} else if err != nil {
				require.Fail(t, err.Error())
			}

			require.JSONEq(t, string(expData), string(actJSON))
		})
	}
}

func TestUnsupportedStatm(t *testing.T) {
	data, err := testdata.ReadFile(filepath.Join("testdata", "unsupported_stmt.sql"))
	require.NoError(t, err)

	_, err = pgproto.ParseFullTyped(data)
	require.ErrorContains(t, err, "only support")
}

func TestNotUsingAlias(t *testing.T) {
	_, err := pgproto.ParseFullTyped([]byte(`SELECT id from foo`))
	require.ErrorContains(t, err, "no alias")
	require.ErrorIs(t, err, pgproto.ErrNoColumnAliasUsed)
}

func TestNotUsingTypeCast(t *testing.T) {
	_, err := pgproto.ParseFullTyped([]byte(`SELECT id as id_1 from foo`))
	require.ErrorContains(t, err, "no type cast")
	require.ErrorIs(t, err, pgproto.ErrColumnWithoutCast)
}

func TestNamedWithoutNumberSuffix(t *testing.T) {
	_, err := pgproto.ParseFullTyped([]byte(`SELECT id AS id1 from foo`))
	require.ErrorContains(t, err, "not named with a number suffix")
	require.ErrorIs(t, err, pgproto.ErrNamedWithoutNumberSuffix)
}

func TestInvalidSuffix(t *testing.T) {
	_, err := pgproto.ParseFullTyped([]byte(`SELECT id AS id_0 from foo`))
	require.ErrorContains(t, err, "invalid number suffix")
	require.ErrorIs(t, err, pgproto.ErrInvalidNumberSuffix)
}

func TestNotNamedIfSuffixNotInt(t *testing.T) {
	_, err := pgproto.ParseFullTyped([]byte(`SELECT id AS id_b from foo`))
	require.ErrorContains(t, err, "not named with a number suffix")
	require.ErrorIs(t, err, pgproto.ErrNamedWithoutNumberSuffix)
}

func TestDuplicateNumberSuffix(t *testing.T) {
	_, err := pgproto.ParseFullTyped([]byte(`SELECT id::text AS id_1, name::uuid AS name_1 from foo`))
	require.ErrorContains(t, err, "duplicate number suffix")
	require.ErrorIs(t, err, pgproto.ErrDuplicateNumberSuffix)
}
