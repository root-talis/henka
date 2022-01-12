#!/usr/bin/env bash
set -e

LINTER_VERSION=1.43.0

type golangci-lint > /dev/null
exists=$?
if [ $exists -ne 0 ]; then
  go install github.com/golangci/golangci-lint/cmd/golangci-lint@v${LINTER_VERSION}
fi

golangci-lint run ./...
