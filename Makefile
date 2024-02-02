.PHONY: generate
generate:
	# Generate the types from the JSON schema
	docker run --rm -v $$PWD/schema.json:/mnt/schema.json omissis/go-jsonschema:0.14.1 --only-models -p migrations --tags json /mnt/schema.json > pkg/migrations/types.go
	
	# Add the license header
	echo "// SPDX-License-Identifier: Apache-2.0" | cat - pkg/migrations/types.go > pkg/migrations/types.go.tmp
	mv pkg/migrations/types.go.tmp pkg/migrations/types.go
