// SPDX-License-Identifier: Apache-2.0

package jsonschema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/stretchr/testify/assert"
	"golang.org/x/tools/txtar"
)

const (
	schemaPath  = "../../schema.json"
	testDataDir = "./testdata"
)

func TestJSONSchemaValidation(t *testing.T) {
	t.Parallel()

	sch := jsonschema.MustCompile(schemaPath)

	files, err := os.ReadDir(testDataDir)
	assert.NoError(t, err)

	for _, file := range files {
		t.Run(file.Name(), func(t *testing.T) {
			ac, err := txtar.ParseFile(filepath.Join(testDataDir, file.Name()))
			assert.NoError(t, err)

			assert.Len(t, ac.Files, 2)

			var v map[string]any
			assert.NoError(t, json.Unmarshal(ac.Files[0].Data, &v))

			shouldValidate, err := strconv.ParseBool(strings.TrimSpace(string(ac.Files[1].Data)))
			assert.NoError(t, err)

			err = sch.Validate(v)
			if shouldValidate && err != nil {
				t.Errorf("%#v", err)
			} else if !shouldValidate && err == nil {
				t.Errorf("expected %q to be invalid", ac.Files[0].Name)
			}
		})
	}
}
