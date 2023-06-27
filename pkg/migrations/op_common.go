package migrations

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Operations []Operation

func TemporaryName(name string) string {
	return "_pgroll_new_" + name
}

func ReadMigrationFile(file string) ([]Operation, error) {
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

	ops := Operations{}
	err = json.Unmarshal(byteValue, &ops)
	if err != nil {
		return nil, err
	}

	return ops, nil
}

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
		var opName string
		var logBody json.RawMessage
		if len(opObj) != 1 {
			return fmt.Errorf("invalid migration: %v", opObj)
		}
		for k, v := range opObj {
			opName = k
			logBody = v
		}

		var item Operation
		switch opName {
		case "create_table":
			item = &OpCreateTable{}
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
