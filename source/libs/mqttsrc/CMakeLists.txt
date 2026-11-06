# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# mqttsrc: subscribe to an external MQTT broker and write into TDengine.
# Ingress direction; unrelated to source/libs/tmqtt (the built-in broker).

# Reuse the cJSON target already vendored in the tree instead of compiling
# its .c file a second time into this library. The community tree adds
# source/ before tools/, so define the target here on first use; the guard in
# tools/taosBenchmark/deps/CMakeLists.txt avoids a duplicate definition.
set(MS_JSON_DIR "${TD_SOURCE_DIR}/tools/taosBenchmark/deps/toolscJson")
if(NOT TARGET toolscJson)
    add_subdirectory("${MS_JSON_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/toolscJson")
endif()

set(MS_SRC
    src/msUtil.c
    src/msCfg.c
    src/msMapper.c
    src/msMqtt.c
    src/msSink.c
    src/msSrc.c
)

add_library(mqttsrc STATIC ${MS_SRC})

target_include_directories(
    mqttsrc
    PUBLIC  "${TD_SOURCE_DIR}/include/libs/mqttsrc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
            "${TD_SOURCE_DIR}/include/client"
            "${TD_SOURCE_DIR}/include"
)
target_link_libraries(mqttsrc PUBLIC toolscJson Threads::Threads)

# Optional TLS (OpenSSL). Without it, "mqtt.tls.enabled" is rejected at config
# load time instead of silently sending credentials in clear text.
option(MQTTSRC_ENABLE_TLS "Build mqttsrc with TLS support (OpenSSL)" OFF)
if(MQTTSRC_ENABLE_TLS)
    find_package(OpenSSL REQUIRED)
    target_compile_definitions(mqttsrc PRIVATE MS_WITH_TLS=1)
    target_link_libraries(mqttsrc PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

if(TD_WINDOWS)
    target_link_libraries(mqttsrc PUBLIC ws2_32)
endif()

add_executable(taosmqttsrc src/msMain.c)
target_link_libraries(taosmqttsrc PRIVATE mqttsrc ${TAOS_LIB})

# libtaos is installed in ../driver with an @rpath install name on macOS;
# without this fix the executable cannot locate it after installation.
if(TD_DARWIN)
    td_fix_macos_client_libtaos_dep(taosmqttsrc)
endif()

# The project builds production targets with -Werror but without -Wall/-Wextra.
# Opt in for local/CI static checks without changing the default build flags.
option(MQTTSRC_STRICT_WARNINGS "Build mqttsrc with -Wall -Wextra" OFF)
if(MQTTSRC_STRICT_WARNINGS AND CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(mqttsrc PRIVATE -Wall -Wextra)
    target_compile_options(taosmqttsrc PRIVATE -Wall -Wextra)
endif()

if(${BUILD_TEST})
    add_subdirectory(test)
endif()
