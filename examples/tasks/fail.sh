#!/bin/sh
set -eu

name="${1:-retryable-failure}"
printf '[%s] started; this task fails intentionally\n' "$name"
printf '[%s] returning exit code 7 to demonstrate retry\n' "$name" >&2
exit 7
