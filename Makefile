.PHONY: generate
generate:
	# Generate the types from the JSON schema
	go-jsonschema --only-models -p migrations --tags json schema.json > pkg/migrations/types.go
	
	# Add the license header
	echo "// SPDX-License-Identifier: Apache-2.0" | cat - pkg/migrations/types.go > pkg/migrations/types.go.tmp
	mv pkg/migrations/types.go.tmp pkg/migrations/types.go
