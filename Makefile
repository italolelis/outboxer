NO_COLOR=\033[0m
OK_COLOR=\033[32;01m
ERROR_COLOR=\033[31;01m
WARN_COLOR=\033[33;01m
SERVICE_NAME=dna-srv

.PHONY: all test lint
all: test

test:
	@echo "$(OK_COLOR)==> Running tests$(NO_COLOR)"
	@go test -v -cover -covermode=atomic -coverprofile=tests.out ./...

generate:
	@go generate ./...

test-integration:
	@echo "$(OK_COLOR)==> Running tests$(NO_COLOR)"
	@go test --tags=integration -v -cover -covermode=atomic -coverprofile=tests.out ./...


setup:
	@echo "$(OK_COLOR)==> Setting up deps$(NO_COLOR)"
	@awslocal kinesis create-stream --stream-name test --shard-count 1
