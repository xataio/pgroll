.PHONY: pgroll generate format lint examples test install-license-checker license

pgroll:
	go build

clean:
	go clean

format:
	# Format JSON schema
	docker run --rm -v $$PWD/schema.json:/mnt/schema.json node:alpine npx prettier /mnt/schema.json --parser json --tab-width 2 --single-quote --trailing-comma all --no-semi --arrow-parens always --print-width 120 --write
	# Format embedded SQL
	docker run --rm -v $$PWD/pkg/state/init.sql:/data/init.sql backplane/pgformatter --inplace /data/init.sql
	# Run gofumpt
	gofumpt -w .

generate:
	# Generate the types from the JSON schema
	docker run --rm -v $$PWD/schema.json:/mnt/schema.json omissis/go-jsonschema:0.17.0 --only-models -p migrations --tags json,yaml /mnt/schema.json > pkg/migrations/types.go
	# Add the license header to the generated type file
	echo "// SPDX-License-Identifier: Apache-2.0" | cat - pkg/migrations/types.go > pkg/migrations/types.go.tmp
	mv pkg/migrations/types.go.tmp pkg/migrations/types.go
	# Generate the cli-definition.json file
	go run tools/build-cli-definition.go

lint:
	golangci-lint --config=.golangci.yml run

ledger:
	cd examples && ls > .ledger

examples: ledger
	@go build
	@./pgroll init
	@./pgroll migrate examples --complete
	@go clean

test:
	go test ./...

bench:
	go test ./internal/benchmarks -v -benchtime=1x -bench .

install-license-checker:
	if [ ! -f ./bin/license-header-checker ]; then curl -s https://raw.githubusercontent.com/lluissm/license-header-checker/master/install.sh | bash; fi

license: install-license-checker
	./bin/license-header-checker -a -r .github/license-header.txt . go
