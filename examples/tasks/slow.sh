#!/bin/sh
set -eu

name="${1:-timeout-task}"
duration="${2:-10}"
printf '[%s] started; sleeping for %ss\n' "$name" "$duration"
sleep "$duration"
printf '[%s] completed\n' "$name"
