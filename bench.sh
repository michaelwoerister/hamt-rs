#!/bin/sh

set -v
set -e

# Make sure dependency binaries are available
cargo clean
cargo build --release

# Get the filename of the rand library
RAND_LIB=`find . -wholename "*release/deps*librand*"`
LIBC_LIB=`find . -wholename "*release/deps*liblibc*"`

# Build the benchmarking binary
mkdir -p benchmark

rustc "src/lib.rs" \
      --crate-name "hamt" \
      --crate-type lib \
      -C opt-level=3 \
      -C target-feature=+popcnt \
      -o "benchmark/libhamt.rlib" \
      -L dependency="target/release" \
      -L dependency="target/release/deps" \
      --extern libc="$LIBC_LIB" \
      --extern rand="$RAND_LIB"

rustc "benches/benches.rs" \
      --crate-name "benches" \
      --crate-type bin \
      -C opt-level=3 \
      -C lto \
      -C target-feature=+popcnt \
      --test \
      -o "benchmark/hamt-bench" \
      -L dependency="target/release" \
      -L dependency="target/release/deps" \
      --extern libc="$LIBC_LIB" \
      --extern rand="$RAND_LIB" \
      --extern hamt="benchmark/libhamt.rlib"

./benchmark/hamt-bench --bench | python ./gen-perf-tables.py
