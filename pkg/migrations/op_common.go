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
	OpNameCreateTable OpName = "create_table"
	OpNameRenameTable OpName = "rename_table"
	OpNameAddColumn   OpName = "add_column"
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

		case OpNameAddColumn:
			item = &OpAddColumn{}

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

		var opName OpName
		switch op.(type) {
		case *OpCreateTable:
			opName = OpNameCreateTable

		case *OpRenameTable:
			opName = OpNameRenameTable

		case *OpAddColumn:
			opName = OpNameAddColumn

		default:
			panic(fmt.Errorf("unknown operation for %T", op))
		}

		buf.WriteString(`{"`)
		buf.WriteString(string(opName))
		buf.WriteString(`":`)
		if err := enc.Encode(op); err != nil {
			return nil, fmt.Errorf("unable to encode op [%v]: %w", i, err)
		}
		buf.WriteByte('}')
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}
