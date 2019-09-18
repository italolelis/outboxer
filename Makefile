NO_COLOR=\033[0m
OK_COLOR=\033[32;01m
ERROR_COLOR=\033[31;01m
WARN_COLOR=\033[33;01m
SERVICE_NAME=dna-srv

.PHONY: all test lint
all: test

test: lint
	@echo "$(OK_COLOR)==> Running tests$(NO_COLOR)"
	@go test -v -cover -covermode=atomic -coverprofile=tests.out ./...

lint: tools.golangci-lint
	@echo "$(OK_COLOR)==> checking code style with 'golangci-lint' tool$(NO_COLOR)"
	@golangci-lint run

#---------------
#-- tools
#---------------

.PHONY: tools tools.golangci-lint
tools: tools.golangci-lint

tools.golangci-lint:
	@command -v golangci-lint >/dev/null ; if [ $$? -ne 0 ]; then \
		echo "$(OK_COLOR)==> installing golangci-lint$(NO_COLOR)"; \
		curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s v1.17.1; \
		mv ./bin/golangci-lint /usr/local/bin; \
	fi
