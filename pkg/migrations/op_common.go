package migrations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type OpName string

const (
	OpNameCreateTable    OpName = "create_table"
	OpNameRenameTable    OpName = "rename_table"
	OpNameDropTable      OpName = "drop_table"
	OpNameAddColumn      OpName = "add_column"
	OpNameDropColumn     OpName = "drop_column"
	OpNameAlterColumn    OpName = "alter_column"
	OpNameCreateIndex    OpName = "create_index"
	OpNameDropIndex      OpName = "drop_index"
	OpNameDropConstraint OpName = "drop_constraint"
	OpRawSQLName         OpName = "sql"

	// Internal operation types used by `alter_column`
	OpNameRenameColumn       OpName = "rename_column"
	OpNameSetUnique          OpName = "set_unique"
	OpNameSetNotNull         OpName = "set_not_null"
	OpNameSetForeignKey      OpName = "set_foreign_key"
	OpNameSetCheckConstraint OpName = "set_check_constraint"
	OpNameChangeType         OpName = "change_type"
)

func TemporaryName(name string) string {
	return "_pgroll_new_" + name
}

func ReadMigrationFile(file string) (*Migration, error) {
	// read operations from file
	jsonFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer jsonFile.Close()

	// read our opened xmlFile as a byte array.
	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		return nil, err
	}

	mig := Migration{}
	err = json.Unmarshal(byteValue, &mig)
	if err != nil {
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

		case OpNameDropConstraint:
			item = &OpDropConstraint{}

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

		default:
			return fmt.Errorf("unknown migration type: %v", opName)
		}

		if err := json.Unmarshal(logBody, item); err != nil {
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

	case *OpDropConstraint:
		return OpNameDropConstraint

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

	}

	panic(fmt.Errorf("unknown operation for %T", op))
}
