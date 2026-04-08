cmake_minimum_required(VERSION 3.14)
project(kepler_logging)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# === Paths ===
# Assuming that NuRaft and RocksDB is already built and available at the specified paths
set(NURAFT_LIB  ${CMAKE_SOURCE_DIR}/NuRaft/build/libnuraft.a)
set(ROCKSDB_LIB ${CMAKE_SOURCE_DIR}/rocksdb/librocksdb.a)

# === Imported targets ===
add_library(nuraft  STATIC IMPORTED)
add_library(rocksdb STATIC IMPORTED)

set_target_properties(nuraft  PROPERTIES IMPORTED_LOCATION ${NURAFT_LIB})
set_target_properties(rocksdb PROPERTIES IMPORTED_LOCATION ${ROCKSDB_LIB})

target_include_directories(nuraft INTERFACE
    ${CMAKE_SOURCE_DIR}/NuRaft/include
    ${CMAKE_SOURCE_DIR}/NuRaft/include/libnuraft
    ${CMAKE_SOURCE_DIR}/NuRaft/tests
)
target_include_directories(rocksdb INTERFACE
    ${CMAKE_SOURCE_DIR}/rocksdb/include
)

# === System dependencies ===
find_package(Threads REQUIRED)
find_package(ZLIB    REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost   REQUIRED)
find_package(gflags  REQUIRED)
find_package(GTest   REQUIRED)

# RocksDB requires these on Linux
find_library(LZ4_LIB    lz4)
find_library(SNAPPY_LIB snappy)
find_library(ZSTD_LIB   zstd)
find_library(BZ2_LIB    bz2)

target_link_libraries(rocksdb INTERFACE
    Threads::Threads
    ZLIB::ZLIB
    OpenSSL::SSL OpenSSL::Crypto
    ${LZ4_LIB} ${SNAPPY_LIB} ${ZSTD_LIB} ${BZ2_LIB}
    dl
)

target_link_libraries(nuraft INTERFACE
    Threads::Threads
    ZLIB::ZLIB
    OpenSSL::SSL OpenSSL::Crypto
    dl
)

set(KEPLER_LOGGING_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
add_subdirectory(src)

