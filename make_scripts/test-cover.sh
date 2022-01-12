#!/usr/bin/env bash
set -e
set -o pipefail

go test ./... -coverprofile .coverage.out
go tool cover -func .coverage.out
