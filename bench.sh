#!/bin/sh

set -v
set -e

# Make sure dependency binaries are available
cargo clean
cargo build --release --features="rust_alloc hashmap_default_hasher"

# Get the filename of the rand library
RAND_LIB=`find . -wholename "*release/deps*librand*"`
LIBC_LIB=`find . -wholename "*release/deps*liblibc*"`

# Build the benchmarking binary
mkdir -p benchmark

rustc "src/lib.rs" \
      --crate-name "hamt_rs" \
      --crate-type lib \
      -C opt-level=3 \
      -C target-feature=+popcnt \
      -o "benchmark/libhamt.rlib" \
      -L dependency="target/release" \
      -L dependency="target/release/deps" \
      --extern libc="$LIBC_LIB" \
      --extern rand="$RAND_LIB" \
      --cfg feature=\"rust_alloc\" \
      --cfg feature=\"hashmap_default_hasher\"


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
      --extern hamt_rs="benchmark/libhamt.rlib" \
      --cfg feature=\"hashmap_default_hasher\"

./benchmark/hamt-bench --bench | python ./gen-perf-tables.py
