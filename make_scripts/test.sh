#!/usr/bin/env bash
set -e
set -o pipefail

go test ./... | { grep -v 'no test files'; true; }
