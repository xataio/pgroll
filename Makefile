.PHONY: generate lint examples test

generate:
	# Format JSON schema
	docker run --rm -v $$PWD/schema.json:/mnt/schema.json node:alpine npx prettier /mnt/schema.json --parser json --tab-width 2 --single-quote --trailing-comma all --no-semi --arrow-parens always --print-width 120 > schema.json.tmp
	mv schema.json.tmp schema.json

	# Generate the types from the JSON schema
	# Temporarily use the `surjection/go-jsonschema` image because we need https://github.com/omissis/go-jsonschema/pull/220
	# Use the official `omissis/gojsonschema` image once 0.17.0 is released.
	docker run --rm -v $$PWD/schema.json:/mnt/schema.json surjection/go-jsonschema:0.16.1 --only-models -p migrations --tags json /mnt/schema.json > pkg/migrations/types.go
	
	# Add the license header
	echo "// SPDX-License-Identifier: Apache-2.0" | cat - pkg/migrations/types.go > pkg/migrations/types.go.tmp
	mv pkg/migrations/types.go.tmp pkg/migrations/types.go

lint:
	golangci-lint --config=.golangci.yml run

examples:
	@go build
	@./pgroll init
	@for file in examples/*.json; do \
	    if [ -f $$file ]; then \
	        ./pgroll start --complete $$file; \
	    fi \
	done
	@go clean

test:
	go test ./...

bench:
	go test ./internal/benchmarks -v -benchtime=1x -bench .
