#!/usr/bin/env bash
set -e
set -o pipefail

go test ./... -test.short | { grep -v 'no test files'; true; }
