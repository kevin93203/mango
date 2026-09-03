#!/bin/bash

go run ./examples/api --port 9000 --interval 3s &
pid1=$!

go run ./examples/api --port 9090 --interval 3s &
pid2=$!

trap 'kill "$pid1" "$pid2" 2>/dev/null; wait' TERM INT EXIT

wait "$pid1" "$pid2"