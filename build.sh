#!/bin/bash

sudo dnf install libzstd-devel snappy-devel bzip2-devel lz4-devel zstd-devel gflags-devel gtest-devel cmake -y

git submodule update --init --recursive

PROJECT_ROOT=$(pwd)
CPUS=$(nproc)

# Build NuRaft
cd NuRaft
./prepare.sh

mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=OFF ..
make static_lib -j $((CPUS/2))

# Build rocksdb
cd $PROJECT_ROOT
cd rocksdb
make static_lib -j $((CPUS/2))

# Build project
cd $PROJECT_ROOT
mkdir build
cd build
cmake ..
make -j $((CPUS/2))
