#!/bin/sh

set -v
set -e

# Make sure dependency binaries are available
cargo build --release

# Get the filename of the rand library
RAND_LIB=`find . -wholename "*release*librand*"`

# Build the benchmarking binary
mkdir -p benchmark
rustc -C lto \
      -C opt-level=3 \
      -C target-feature=+popcnt \
      -o./benchmark/hamt-bench \
      --test \
      -L dependency=./target/release \
      -L dependency=./target/release/deps \
      --extern rand=$RAND_LIB \
      ./src/lib.rs

./benchmark/hamt-bench --bench
