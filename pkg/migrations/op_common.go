// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/yaml"
)

type OpName string

const (
	OpNameCreateTable               OpName = "create_table"
	OpNameRenameTable               OpName = "rename_table"
	OpNameRenameColumn              OpName = "rename_column"
	OpNameDropTable                 OpName = "drop_table"
	OpNameAddColumn                 OpName = "add_column"
	OpNameDropColumn                OpName = "drop_column"
	OpNameAlterColumn               OpName = "alter_column"
	OpNameCreateIndex               OpName = "create_index"
	OpNameDropIndex                 OpName = "drop_index"
	OpNameRenameConstraint          OpName = "rename_constraint"
	OpNameDropConstraint            OpName = "drop_constraint"
	OpNameSetReplicaIdentity        OpName = "set_replica_identity"
	OpNameDropMultiColumnConstraint OpName = "drop_multicolumn_constraint"
	OpRawSQLName                    OpName = "sql"
	OpCreateConstraintName          OpName = "create_constraint"
)

const (
	temporaryPrefix = "_pgroll_new_"
	deletedPrefix   = "_pgroll_del_"
)

// TemporaryName returns a temporary name for a given name.
func TemporaryName(name string) string {
	return temporaryPrefix + name
}

// DeletionName returns the deleted name for a given name.
func DeletionName(name string) string {
	return deletedPrefix + name
}

// CollectFilesFromDir returns a list of migration files in a directory.
// The files are ordered based on the filename without the extension name.
func CollectFilesFromDir(dir fs.FS) ([]string, error) {
	supportedExtensionsGlob := []string{"*.json", "*.yml", "*.yaml"}
	var migrationFiles []string
	for _, glob := range supportedExtensionsGlob {
		files, err := fs.Glob(dir, glob)
		if err != nil {
			return nil, fmt.Errorf("reading directory: %w", err)
		}
		migrationFiles = append(migrationFiles, files...)
	}

	// Order slice based on filename without extension
	slices.SortFunc(migrationFiles, func(f1, f2 string) int {
		return strings.Compare(filepath.Base(f1), filepath.Base(f2))
	})

	return migrationFiles, nil
}

// ReadMigration opens the migration file and reads the migration as a
// RawMigration.
func ReadRawMigration(dir fs.FS, filename string) (*RawMigration, error) {
	file, err := dir.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("opening migration file: %w", err)
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	mig := RawMigration{}
	switch filepath.Ext(filename) {
	case ".json":
		dec := json.NewDecoder(bytes.NewReader(byteValue))
		dec.DisallowUnknownFields()
		err = dec.Decode(&mig)
	case ".yaml", ".yml":
		err = yaml.UnmarshalStrict(byteValue, &mig)
	}
	if err != nil {
		return nil, fmt.Errorf("reading migration file: %w", err)
	}

	if mig.Name == "" {
		// Extract base filename without extension as the default migration name
		mig.Name = strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
	}

	return &mig, nil
}

// ParseMigration converts a RawMigration to a fully parsed Migration
func ParseMigration(raw *RawMigration) (*Migration, error) {
	var ops Operations
	if err := json.Unmarshal(raw.Operations, &ops); err != nil {
		return nil, fmt.Errorf("parsing operations: %w", err)
	}

	return &Migration{
		Name:       raw.Name,
		Operations: ops,
	}, nil
}

// ReadMigration reads and parses a migration file
func ReadMigration(dir fs.FS, filename string) (*Migration, error) {
	raw, err := ReadRawMigration(dir, filename)
	if err != nil {
		return nil, err
	}

	return ParseMigration(raw)
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
			return fmt.Errorf("multiple keys in operation object at index %d: %v",
				i, strings.Join(slices.Collect(maps.Keys(opObj)), ", "))
		}
		for k, v := range opObj {
			opName = OpName(k)
			logBody = v
		}

		item, err := operationFromName(opName)
		if err != nil {
			return err
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

	case *OpRenameColumn:
		return OpNameRenameColumn

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

	case *OpRawSQL:
		return OpRawSQLName

	case *OpCreateConstraint:
		return OpCreateConstraintName

	case *OpDropMultiColumnConstraint:
		return OpNameDropMultiColumnConstraint

	}

	panic(fmt.Errorf("unknown operation for %T", op))
}

func operationFromName(name OpName) (Operation, error) {
	switch name {
	case OpNameCreateTable:
		return &OpCreateTable{}, nil

	case OpNameRenameTable:
		return &OpRenameTable{}, nil

	case OpNameDropTable:
		return &OpDropTable{}, nil

	case OpNameAddColumn:
		return &OpAddColumn{}, nil

	case OpNameRenameColumn:
		return &OpRenameColumn{}, nil

	case OpNameDropColumn:
		return &OpDropColumn{}, nil

	case OpNameRenameConstraint:
		return &OpRenameConstraint{}, nil

	case OpNameDropConstraint:
		return &OpDropConstraint{}, nil

	case OpNameSetReplicaIdentity:
		return &OpSetReplicaIdentity{}, nil

	case OpNameAlterColumn:
		return &OpAlterColumn{}, nil

	case OpNameCreateIndex:
		return &OpCreateIndex{}, nil

	case OpNameDropIndex:
		return &OpDropIndex{}, nil

	case OpRawSQLName:
		return &OpRawSQL{}, nil

	case OpCreateConstraintName:
		return &OpCreateConstraint{}, nil

	case OpNameDropMultiColumnConstraint:
		return &OpDropMultiColumnConstraint{}, nil

	}
	return nil, fmt.Errorf("unknown migration type: %v", name)
}

// WriteAsJSON writes the migration to the given writer in JSON format
func (m *RawMigration) WriteAsJSON(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")

	return encoder.Encode(m)
}

// WriteAsYAML writes the migration to the given writer in YAML format
func (m *RawMigration) WriteAsYAML(w io.Writer) error {
	yml, err := yaml.Marshal(m)
	if err != nil {
		return err
	}

	_, err = w.Write(yml)
	return err
}
