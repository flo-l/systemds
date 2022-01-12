# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

cmake_minimum_required(VERSION 3.13)

project(MA_LACKNER VERSION 0.2 LANGUAGES CXX)

add_library(seal_implementation SHARED src/org_apache_sysds_runtime_controlprogram_paramserv_homomorphicEncryption_SEAL.cpp)
target_include_directories(seal_implementation PUBLIC include /usr/lib/jvm/java-11-openjdk-11.0.13.0.8-2.fc34.x86_64/include /usr/lib/jvm/java-11-openjdk-11.0.13.0.8-2.fc34.x86_64/include/linux)

# Import Microsoft SEAL
find_package(SEAL 3.7.1 EXACT REQUIRED)
target_link_libraries(seal_implementation SEAL::seal)
target_compile_options(seal_implementation PRIVATE -Wall -Wextra -Wconversion -Wsign-conversion -pedantic-errors)