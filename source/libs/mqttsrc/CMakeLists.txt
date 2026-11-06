# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# mqttsrc: subscribe to an external MQTT broker and write into TDengine.
# Ingress direction; unrelated to source/libs/tmqtt (the built-in broker).

# Reuse the cJSON copy already vendored in the tree. This keeps the module
# free of any additional ExternalProject / conan download.
set(MS_JSON_DIR "${TD_SOURCE_DIR}/tools/taosBenchmark/deps/toolscJson")
set(MS_JSON_SRC "${MS_JSON_DIR}/src/toolscJson.c")

set(MS_SRC
    src/msUtil.c
    src/msCfg.c
    src/msMapper.c
    src/msMqtt.c
    src/msSink.c
    src/msSrc.c
    ${MS_JSON_SRC}
)

add_library(mqttsrc STATIC ${MS_SRC})

target_include_directories(
    mqttsrc
    PUBLIC  "${TD_SOURCE_DIR}/include/libs/mqttsrc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
            "${MS_JSON_DIR}/inc"
            "${TD_SOURCE_DIR}/include/client"
            "${TD_SOURCE_DIR}/include"
)

if(TD_WINDOWS)
    target_link_libraries(mqttsrc PUBLIC ws2_32)
endif()

add_executable(taosmqttsrc src/msMain.c)
target_link_libraries(taosmqttsrc PRIVATE mqttsrc ${TAOS_LIB})

if(${BUILD_TEST})
    add_subdirectory(test)
endif()
