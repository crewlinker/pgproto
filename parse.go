// Package pgproto implements code generation for protobuf-powered clients for Postgres.
package pgproto

import (
	"encoding/json"
	"errors"
	"fmt"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	"github.com/samber/lo"
)

// Output describe the output from an action.
type Output struct{}

// Action describes an action we support.
type Action interface{ isAction() }

type (
	// SelectAction describes an action that selects data.
	SelectAction struct {
		Outputs []*Output
	}

	// UpdateAction describes an action of updating data.
	UpdateAction struct {
		Outputs []*Output
	}

	// InsertAction describes an action of inserting data.
	InsertAction struct {
		Outputs []*Output
	}

	// DeleteAction describes an action of deleting data.
	DeleteAction struct {
		Outputs []*Output
	}
)

func (SelectAction) isAction() {}
func (UpdateAction) isAction() {}
func (InsertAction) isAction() {}
func (DeleteAction) isAction() {}

func parseResultTarget(*pgquery.ResTarget) (*Output, error) {
	return &Output{}, nil
}

func parseSelectStmt(stmt *pgquery.SelectStmt) (action *SelectAction, err error) {
	action = &SelectAction{}
	for _, target := range stmt.GetTargetList() {
		rest := target.GetResTarget()
		if rest == nil {
			panicf(target, "no result target")
		}

		output, perr := parseResultTarget(rest)
		if perr != nil {
			err = errors.Join(err, resTargetErrorf(rest, "failed to parse: %w", err))

			continue
		}

		action.Outputs = append(action.Outputs, output)
	}

	return action, nil
}

func parseInsertStmt(stmt *pgquery.InsertStmt) (action *InsertAction, err error) {
	action = &InsertAction{}
	for _, returning := range stmt.GetReturningList() {
		rest := returning.GetResTarget()
		if rest == nil {
			panicf(returning, "no result target")
		}

		output, perr := parseResultTarget(rest)
		if perr != nil {
			err = errors.Join(err, resTargetErrorf(rest, "failed to parse: %w", err))

			continue
		}

		action.Outputs = append(action.Outputs, output)
	}

	return action, nil
}

func parseDeleteStmt(stmt *pgquery.DeleteStmt) (action *DeleteAction, err error) {
	action = &DeleteAction{}
	for _, returning := range stmt.GetReturningList() {
		rest := returning.GetResTarget()
		if rest == nil {
			panicf(returning, "no result target")
		}

		output, perr := parseResultTarget(rest)
		if perr != nil {
			err = errors.Join(err, resTargetErrorf(rest, "failed to parse: %w", err))

			continue
		}

		action.Outputs = append(action.Outputs, output)
	}

	return action, nil
}

func parseUpdateStmt(stmt *pgquery.UpdateStmt) (action *UpdateAction, err error) {
	action = &UpdateAction{}
	for _, returning := range stmt.GetReturningList() {
		rest := returning.GetResTarget()
		if rest == nil {
			panicf(returning, "no result target")
		}

		output, perr := parseResultTarget(rest)
		if perr != nil {
			err = errors.Join(err, resTargetErrorf(rest, "failed to parse: %w", err))

			continue
		}

		action.Outputs = append(action.Outputs, output)
	}

	return action, nil
}

func parseStmt(rstmt *pgquery.RawStmt) (action Action, err error) {
	stmt := rstmt.GetStmt()
	sel, ins, upd, del := stmt.GetSelectStmt(),
		stmt.GetInsertStmt(),
		stmt.GetUpdateStmt(),
		stmt.GetDeleteStmt()

	switch {
	case sel != nil:
		action, err = parseSelectStmt(sel)
	case ins != nil:
		action, err = parseInsertStmt(ins)
	case upd != nil:
		action, err = parseUpdateStmt(upd)
	case del != nil:
		action, err = parseDeleteStmt(del)
	default:
		// @TODO support UPSERT and MERGE
		return nil, stmtErrorf(rstmt, "only support SELECT, INSERT, UPDATE or DELETE statements")
	}

	if err != nil {
		return nil, stmtErrorf(rstmt, "failed to parse: %w", err)
	}

	return action, nil
}

// ParseFullTyped parses the input SQL into one or more actions. For typing, it does not need to know anything about the
// schema or read from the postgres catalog. Instead, it requires the query SQL to be written in a more explicit way.
// First, it requires all result columns and named arguments in the result to be explicitly typed via typecasts ("::").
// Second, each column in the result set must also be aliased  using the "AS" operation. And finally, each alias and
// named argument must be suffixed with a "_<N>", where N is a long-term fixed integer (>0) that should not change as
// queries evolve over time.
func ParseFullTyped(input []byte) (actions []Action, err error) {
	result, err := pgquery.Parse(string(input))
	if err != nil {
		return nil, fmt.Errorf("failed to parse: %w", err)
	}

	for _, rstmt := range result.GetStmts() {
		action, perr := parseStmt(rstmt)
		if perr != nil {
			err = errors.Join(err, perr)
		} else {
			actions = append(actions, action)
		}
	}

	return actions, err
}

func stmtErrorf(rstmt *pgquery.RawStmt, format string, args ...any) error {
	return fmt.Errorf("statement@%d: %w", rstmt.GetStmtLocation(), fmt.Errorf(format, args...))
}

func resTargetErrorf(rstmt *pgquery.ResTarget, format string, args ...any) error {
	return fmt.Errorf("result_target@%d: %w", rstmt.GetLocation(), fmt.Errorf(format, args...))
}

func panicf(node *pgquery.Node, format string, args ...any) {
	panic(fmt.Sprintf(format, args...) + ": " + sdump(node))
}

func sdump(v any) string {
	return string(lo.Must(json.MarshalIndent(v, "", "  ")))
}
