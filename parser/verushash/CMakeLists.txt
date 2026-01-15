cmake_minimum_required(VERSION 3.12)
project(verus_crypto)

# Set compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")

# Add include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
if(APPLE)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/compat)
endif()

# Add the verus_crypto library
add_library(verus_crypto STATIC
    crypto/haraka.c
    crypto/haraka_portable.c
    crypto/uint256.cpp
    crypto/utilstrencodings.cpp
    crypto/verus_hash.cpp
    crypto/verus_clhash.cpp
    crypto/verus_clhash_portable.cpp
    crypto/ripemd160.cpp
    crypto/sha256.cpp
    support/cleanse.cpp
    blockhash.cpp
    solutiondata.cpp
)

# Set source properties
set_source_files_properties(
    crypto/verus_hash.cpp
    crypto/verus_clhash.cpp
    crypto/haraka.c
    PROPERTIES COMPILE_FLAGS "-m64 -mavx -mpclmul -msse2 -msse3 -mssse3 -msse4 -msse4.1 -msse4.2 -maes -g -fomit-frame-pointer"
)

# optimizations
add_definitions(-O3)

set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/crypto/verus_hash.cpp PROPERTIES COMPILE_FLAGS "-m64 -mavx -mpclmul -msse2 -msse3 -mssse3 -msse4 -msse4.1 -msse4.2 -maes -g -fomit-frame-pointer")
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/crypto/verus_clhash.cpp PROPERTIES COMPILE_FLAGS "-m64 -mavx -mpclmul -msse2 -msse3 -mssse3 -msse4 -msse4.1 -msse4.2 -maes -g -fomit-frame-pointer")
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/crypto/haraka.c PROPERTIES COMPILE_FLAGS "-m64 -mpclmul -mavx -msse2 -msse3 -mssse3 -msse4 -msse4.1 -msse4.2 -maes -g -fomit-frame-pointer")
# Fetch and build Libsodium
include(ExternalProject)

set(LIBSODIUM_VERSION "1.0.18")

if(WIN32)
    set(LIBSODIUM_SOURCE_URL "https://download.libsodium.org/libsodium/releases/libsodium-${LIBSODIUM_VERSION}-stable-msvc.zip")
    set(LIBSODIUM_HASH "c1d48d85c9361e350931ffe5067559cd7405a697c655d26955fb568d1084a5f4")
else()
    set(LIBSODIUM_SOURCE_URL "https://download.libsodium.org/libsodium/releases/libsodium-${LIBSODIUM_VERSION}.tar.gz")
    set(LIBSODIUM_HASH "6f504490b342a4f8a4c4a02fc9b866cbef8622d5df4e5452b46be121e46636c1")
endif()

ExternalProject_Add(
    libsodium
    URL ${LIBSODIUM_SOURCE_URL}
    URL_HASH SHA256=${LIBSODIUM_HASH}
    PREFIX ${CMAKE_BINARY_DIR}/staging/libsodium
    CONFIGURE_COMMAND cd ${CMAKE_BINARY_DIR}/staging/libsodium/src/libsodium/ && ./autogen.sh
                      && ./configure
                          --enable-static
                          --disable-shared
                          --prefix=${CMAKE_BINARY_DIR}/
    BUILD_COMMAND cd ${CMAKE_BINARY_DIR}/staging/libsodium/src/libsodium/ && make
    BUILD_BYPRODUCTS ${CMAKE_BINARY_DIR}/staging/libsodium/src/libsodium/libsodium/.libs/libsodium.a
    INSTALL_COMMAND cd ${CMAKE_BINARY_DIR}/staging/libsodium/src/libsodium && make install)



# Fetch and build Boost
set(BOOST_VERSION "1_72_0")
string(REPLACE "_" "." BOOST_VERSION_DOTTED "${BOOST_VERSION}")
set(BOOST_SOURCE_URL "https://archives.boost.io/release/${BOOST_VERSION_DOTTED}/source/boost_${BOOST_VERSION}.tar.bz2")
set(BOOST_SHA256SUM "59c9b274bc451cf91a9ba1dd2c7fdcaf5d60b1b3aa83f2c9fa143417cc660722")

ExternalProject_Add(
    boost
    URL ${BOOST_SOURCE_URL}
    URL_HASH SHA256=${BOOST_SHA256SUM}
    PREFIX ${CMAKE_BINARY_DIR}/staging/boost

    CONFIGURE_COMMAND ""
    BUILD_COMMAND cd ${CMAKE_BINARY_DIR}/staging/boost/src/boost/ && ./bootstrap.sh
                  --without-icu
                  --with-libraries=system,filesystem
                  && ./b2
                  --prefix=${CMAKE_BINARY_DIR}/
                  install
    BUILD_BYPRODUCTS ${CMAKE_BINARY_DIR}/
    INSTALL_COMMAND ""
)

include_directories(${CMAKE_BINARY_DIR}/include)

# Link Libsodium and Boost to verus_crypto
add_dependencies(verus_crypto libsodium boost)
target_link_libraries(verus_crypto PRIVATE ${LIBSODIUM_LIB}/sodium ${BOOST_LIB}/libboost_system.a ${BOOST_LIB}/libboost_filesystem.a)

# Include common directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
find_package(PkgConfig REQUIRED)

# Specify the output directory for libverus_crypto.a
set_target_properties(verus_crypto PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)

# Print flags and libs
message("-- CXXFLAGS: ${CMAKE_CXX_FLAGS}")
message("-- LIBS: ${LIBS}")

# Include SWIG setup
include(CMakeListsSWIG.txt)