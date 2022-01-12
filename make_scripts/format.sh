#!/usr/bin/env bash
set -e

type goimports > /dev/null
exists=$?
if [ $exists -ne 0 ]; then
  go get github.com/daixiang0/gci
  go get golang.org/x/tools/cmd/goimports
fi

goimports -w .
gci -w -local github.com/root-talis/henka .
