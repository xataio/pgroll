// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

type OpName string

const (
	OpNameCreateTable        OpName = "create_table"
	OpNameRenameTable        OpName = "rename_table"
	OpNameDropTable          OpName = "drop_table"
	OpNameAddColumn          OpName = "add_column"
	OpNameDropColumn         OpName = "drop_column"
	OpNameAlterColumn        OpName = "alter_column"
	OpNameCreateIndex        OpName = "create_index"
	OpNameDropIndex          OpName = "drop_index"
	OpNameRenameConstraint   OpName = "rename_constraint"
	OpNameDropConstraint     OpName = "drop_constraint"
	OpNameSetReplicaIdentity OpName = "set_replica_identity"
	OpRawSQLName             OpName = "sql"
	OpCreateConstraintName   OpName = "create_constraint"

	// Internal operation types used by `alter_column`
	OpNameRenameColumn       OpName = "rename_column"
	OpNameSetUnique          OpName = "set_unique"
	OpNameSetNotNull         OpName = "set_not_null"
	OpNameSetForeignKey      OpName = "set_foreign_key"
	OpNameSetCheckConstraint OpName = "set_check_constraint"
	OpNameChangeType         OpName = "change_type"
)

const temporaryPrefix = "_pgroll_new_"

// TemporaryName returns a temporary name for a given name.
func TemporaryName(name string) string {
	return temporaryPrefix + name
}

// ReadMigration reads a migration from an io.Reader, like a file.
func ReadMigration(r io.Reader) (*Migration, error) {
	byteValue, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(byteValue))
	dec.DisallowUnknownFields()

	mig := Migration{}
	if err = dec.Decode(&mig); err != nil {
		return nil, err
	}

	return &mig, nil
}

// UnmarshalJSON deserializes the list of operations from a JSON array.
func (v *Operations) UnmarshalJSON(data []byte) error {
	var tmp []map[string]json.RawMessage
	if err := json.Unmarshal(data, &tmp); err != nil {
		return nil
	}

	if len(tmp) == 0 {
		*v = Operations{}
		return nil
	}

	ops := make([]Operation, len(tmp))
	for i, opObj := range tmp {
		var opName OpName
		var logBody json.RawMessage
		if len(opObj) != 1 {
			return fmt.Errorf("invalid migration: %v", opObj)
		}
		for k, v := range opObj {
			opName = OpName(k)
			logBody = v
		}

		var item Operation
		switch opName {
		case OpNameCreateTable:
			item = &OpCreateTable{}

		case OpNameRenameTable:
			item = &OpRenameTable{}

		case OpNameDropTable:
			item = &OpDropTable{}

		case OpNameAddColumn:
			item = &OpAddColumn{}

		case OpNameDropColumn:
			item = &OpDropColumn{}

		case OpNameRenameConstraint:
			item = &OpRenameConstraint{}

		case OpNameDropConstraint:
			item = &OpDropConstraint{}

		case OpNameSetReplicaIdentity:
			item = &OpSetReplicaIdentity{}

		case OpNameAlterColumn:
			item = &OpAlterColumn{}

		case OpNameCreateIndex:
			item = &OpCreateIndex{}

		case OpNameDropIndex:
			item = &OpDropIndex{}

		case OpNameSetUnique:
			item = &OpSetUnique{}

		case OpRawSQLName:
			item = &OpRawSQL{}

		case OpCreateConstraintName:
			item = &OpCreateConstraint{}

		default:
			return fmt.Errorf("unknown migration type: %v", opName)
		}

		dec := json.NewDecoder(bytes.NewReader(logBody))
		dec.DisallowUnknownFields()
		if err := dec.Decode(item); err != nil {
			return fmt.Errorf("decode migration [%v]: %w", opName, err)
		}

		ops[i] = item
	}

	*v = ops
	return nil
}

// MarshalJSON serializes the list of operations into a JSON array.
func (v Operations) MarshalJSON() ([]byte, error) {
	if len(v) == 0 {
		return []byte(`[]`), nil
	}

	var buf bytes.Buffer
	buf.WriteByte('[')

	enc := json.NewEncoder(&buf)
	for i, op := range v {
		if i != 0 {
			buf.WriteByte(',')
		}

		buf.WriteString(`{"`)
		buf.WriteString(string(OperationName(op)))
		buf.WriteString(`":`)
		if err := enc.Encode(op); err != nil {
			return nil, fmt.Errorf("unable to encode op [%v]: %w", i, err)
		}
		buf.WriteByte('}')
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

// OperationName returns the name of the operation.
func OperationName(op Operation) OpName {
	switch op.(type) {
	case *OpCreateTable:
		return OpNameCreateTable

	case *OpRenameTable:
		return OpNameRenameTable

	case *OpDropTable:
		return OpNameDropTable

	case *OpAddColumn:
		return OpNameAddColumn

	case *OpDropColumn:
		return OpNameDropColumn

	case *OpRenameConstraint:
		return OpNameRenameConstraint

	case *OpDropConstraint:
		return OpNameDropConstraint

	case *OpSetReplicaIdentity:
		return OpNameSetReplicaIdentity

	case *OpAlterColumn:
		return OpNameAlterColumn

	case *OpCreateIndex:
		return OpNameCreateIndex

	case *OpDropIndex:
		return OpNameDropIndex

	case *OpSetUnique:
		return OpNameSetUnique

	case *OpRawSQL:
		return OpRawSQLName

	case *OpCreateConstraint:
		return OpCreateConstraintName

	}

	panic(fmt.Errorf("unknown operation for %T", op))
}

func ptr[T any](v T) *T {
	return &v
}
