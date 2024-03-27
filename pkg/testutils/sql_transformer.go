// SPDX-License-Identifier: Apache-2.0

package testutils

import "errors"

type MockSQLTransformer struct {
	transformations map[string]string
}

const MockSQLTransformerError = "ERROR"

var ErrMockSQLTransformer = errors.New("SQL transformer error")

// NewMockSQLTransformer creates a MockSQLTransformer with the given transformations.
// The transformations map is a map of input SQL to output SQL. If the output
// SQL is "ERROR", the transformer will return an error on that input.
func NewMockSQLTransformer(ts map[string]string) *MockSQLTransformer {
	return &MockSQLTransformer{
		transformations: ts,
	}
}

// TransformSQL transforms the given SQL string according to the transformations
// provided to the MockSQLTransformer. If the input SQL is not in the transformations
// map, the input SQL is returned unchanged.
func (s *MockSQLTransformer) TransformSQL(sql string) (string, error) {
	out, ok := s.transformations[sql]
	if !ok {
		return sql, nil
	}

	if out == MockSQLTransformerError {
		return "", ErrMockSQLTransformer
	}

	return out, nil
}
