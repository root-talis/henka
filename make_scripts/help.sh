#!/usr/bin/env bash
set -e

echo "Usage:
  make <command>

Commands:
  clean      - remove all temporary files and test cache;
  format     - automatically format source code;
  help       - show this help message;
  lint       - run linters;
  test       - run all tests;
  test-cover - run all tests with coverage;
  test-short - run all tests EXCEPT integration tests;

Have fun!"
