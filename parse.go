// Package pgproto implements code generation for protobuf-powered clients for Postgres.
package pgproto

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	"github.com/samber/lo"
)

// Output describe the output from an action.
type Output struct {
	Number int
	Name   string
	Type   TypeRef
}

// TypeRef references a type.
type TypeRef struct {
	Schema *string
	Name   string
}

// Action describes an action we support.
type Action interface {
	isAction()
	getOutputs() []*Output
}

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

func (SelectAction) isAction()               {}
func (UpdateAction) isAction()               {}
func (InsertAction) isAction()               {}
func (DeleteAction) isAction()               {}
func (a SelectAction) getOutputs() []*Output { return a.Outputs }
func (a UpdateAction) getOutputs() []*Output { return a.Outputs }
func (a InsertAction) getOutputs() []*Output { return a.Outputs }
func (a DeleteAction) getOutputs() []*Output { return a.Outputs }

// ErrNoColumnAliasUsed is returned when parsing a result target but it has no explicitly named with an alias.
var ErrNoColumnAliasUsed = errors.New(`no alias for column in result set, use "AS" to define the alias`)

// ErrColumnWithoutCast is returned when parsing a result target but the colun is not type casted.
var ErrColumnWithoutCast = errors.New(`no type cast for column in result set, use "::" to declare the type`)

// ErrTypeCastInvalid is returned when the type cast is invalid.
var ErrTypeCastInvalid = errors.New(`invalid type cast for column, must be "::<name>" or "::<schema>.<name>" `)

// svalString returns a string.
func svalString(n interface{ GetString_() *pgquery.String }) string {
	str := n.GetString_()
	if str == nil {
		panicf(nil, "no string value in string")
	}

	return str.GetSval()
}

func parseResultTarget(stmt interface{ GetResTarget() *pgquery.ResTarget }) (out *Output, err error) {
	rtgt := stmt.GetResTarget()
	if rtgt == nil {
		panicf(nil, "no result target")
	}

	// @TODO better errors by menting the column name. as in for SELECT id FROM foo; it should mention 'id'
	// in the error. For furthe errors. BUT this requires de-nesting the casting stack, check for a troublesome
	// example in the "double_result_cast.sql" example. If we have that, we should replace column '%s' shown
	// below to mention the column name, not the alias.

	out = &Output{}
	out.Name = rtgt.GetName()
	if out.Name == "" {
		return nil, resTargetErrorf(rtgt, "%w", ErrNoColumnAliasUsed)
	}

	out.Number, err = numberedName(out.Name)
	if err != nil {
		return nil, resTargetErrorf(rtgt, "%w", err)
	}

	val := rtgt.GetVal()
	if val == nil {
		panicf(nil, "result target without value (val)")
	}

	cast := val.GetTypeCast()
	if cast == nil {
		return nil, resTargetErrorf(rtgt, "alias '%s': %w", out.Name, ErrColumnWithoutCast)
	}

	typeName := cast.GetTypeName()
	if typeName == nil {
		panicf(val, "type cast without type name")
	}

	typeNameParts := typeName.GetNames()
	switch len(typeNameParts) {
	case 1: // not fully qualified, e.g:  SELECT '123'::int4;
		out.Type.Name = svalString(typeNameParts[0])
	case 2: // fully qualified, e.g:      SELECT '123'::pg_catalog.int4;
		schemaStr := svalString(typeNameParts[0])
		out.Type.Schema = &schemaStr
		out.Type.Name = svalString(typeNameParts[1])
	default:
		return nil, resTargetErrorf(rtgt, "alias '%s': %w, number of parts: %d",
			out.Name, ErrTypeCastInvalid, len(typeNameParts))
	}

	for _, part := range typeName.GetNames() {
		partStr := part.GetString_()
		if partStr == nil {
			panicf(part, "type cast name part is not a string")
		}
	}

	return out, nil
}

func parseSelectStmt(stmt *pgquery.SelectStmt) (action *SelectAction, err error) {
	action = &SelectAction{}
	for _, target := range stmt.GetTargetList() {
		output, perr := parseResultTarget(target)
		if perr != nil {
			err = errors.Join(err, perr)

			continue
		}

		action.Outputs = append(action.Outputs, output)
	}

	return
}

func parseInsertStmt(stmt *pgquery.InsertStmt) (action *InsertAction, err error) {
	action = &InsertAction{}
	for _, returning := range stmt.GetReturningList() {
		output, perr := parseResultTarget(returning)
		if perr != nil {
			err = errors.Join(err, perr)

			continue
		}

		action.Outputs = append(action.Outputs, output)
	}

	return
}

func parseDeleteStmt(stmt *pgquery.DeleteStmt) (action *DeleteAction, err error) {
	action = &DeleteAction{}
	for _, returning := range stmt.GetReturningList() {
		output, perr := parseResultTarget(returning)
		if perr != nil {
			err = errors.Join(err, perr)

			continue
		}

		action.Outputs = append(action.Outputs, output)
	}

	return
}

func parseUpdateStmt(stmt *pgquery.UpdateStmt) (action *UpdateAction, err error) {
	action = &UpdateAction{}
	for _, returning := range stmt.GetReturningList() {
		output, perr := parseResultTarget(returning)
		if perr != nil {
			err = errors.Join(err, perr)

			continue
		}

		action.Outputs = append(action.Outputs, output)
	}

	return action, nil
}

// ErrDuplicateNumberSuffix is returned when a number suffix for a name is used twice.
var ErrDuplicateNumberSuffix = errors.New("duplicate number suffix")

func checkAction(action Action) error {
	outputsByNumber := map[int]*Output{}
	for _, output := range action.getOutputs() {
		if existing, exists := outputsByNumber[output.Number]; exists {
			return fmt.Errorf("%w, %d is already used by: %s", ErrDuplicateNumberSuffix, output.Number, existing.Name)
		}

		outputsByNumber[output.Number] = output
	}

	return nil
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
		return nil, stmtErrorf(rstmt, "%w", err)
	}

	// @TODO walk the stmt node tree to find AExpr nodes where the "name" == "@" and "rexpr" is not nil. Take care of
	// differencees in the AST tree when using "CAST .. AS text" and "::text"

	if err := checkAction(action); err != nil {
		return nil, stmtErrorf(rstmt, "%w", err)
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

// ErrNamedWithoutNumberSuffix is returned when a column name (alias) does not have a numbered suffix.
var ErrNamedWithoutNumberSuffix = errors.New("not named with a number suffix, add _<N> at the end")

// ErrInvalidNumberSuffix is returned when the name has a number suffix, but its invalid.
var ErrInvalidNumberSuffix = errors.New("invalid number suffix for name, must be > 0")

// numberedName extracts the number at the end of a string separated by an underscores.
func numberedName(name string) (int, error) {
	lastUnderscore := strings.LastIndex(name, "_")
	if lastUnderscore == -1 || lastUnderscore == len(name)-1 {
		return 0, ErrNamedWithoutNumberSuffix
	}

	numStr := name[lastUnderscore+1:]

	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, ErrNamedWithoutNumberSuffix
	}

	if num < 1 {
		return 0, ErrInvalidNumberSuffix
	}

	return num, nil
}
