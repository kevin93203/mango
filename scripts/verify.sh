#!/usr/bin/env bash
set -euo pipefail

goos="$(go env GOOS)"

run() {
	local label=$1
	shift
	printf '\n==> %s\n' "$label"
	"$@"
}

# Windows cannot remove files that another concurrently running test package
# still has open. Keep package test processes serialized there; this also
# avoids starving the short daemon readiness and health-check timeouts while
# the SQLite and archive suites run.
go_test_args=(-count=1)
if [[ "$goos" == "windows" ]]; then
	go_test_args+=(-p 1)
fi
run "Go tests" go test "${go_test_args[@]}" ./...
run "Go vet" go vet ./...
run "Rust formatting" cargo fmt --manifest-path mango-shim/Cargo.toml -- --check

if [[ "$goos" == "darwin" ]]; then
	run "Rust tests" cargo test --manifest-path mango-shim/Cargo.toml -- --test-threads=1
else
	run "Rust tests" cargo test --manifest-path mango-shim/Cargo.toml
fi

run "Rust clippy" cargo clippy --manifest-path mango-shim/Cargo.toml --all-targets -- -D warnings
run "Build CLI" go build ./cmd/mango
run "Build daemon" go build ./cmd/mangod
run "Build release shim" cargo build --release --manifest-path mango-shim/Cargo.toml

if [[ "$goos" != "windows" ]]; then
	export MANGO_SHIM_TEST_BINARY="$(pwd)/mango-shim/target/release/mango-shim"
	run "Go/Rust shim integration" go test ./internal/shim -run TestRustShimClientStartOrAttach -count=1
	run "Daemon shim integration" go test ./internal/daemon -run 'TestDaemon(ReattachesRustShimAfterControlPlaneShutdown|RecreatesServiceAfterShimKill)' -count=1
fi
