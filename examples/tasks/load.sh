#!/bin/sh
set -eu

name="${1:-shell-task}"
printf '[%s] started\n' "$name"
printf '[%s] loading sample records\n' "$name"
sleep 1
printf '[%s] completed\n' "$name"
