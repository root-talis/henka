#!/usr/bin/env bash

go test -coverprofile .coverage.out
go tool cover -func .coverage.out

