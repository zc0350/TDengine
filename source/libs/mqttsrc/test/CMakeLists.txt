# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Pure logic tests: no MQTT broker and no TDengine server required.
add_executable(msUnitTest msUnitTest.c)
target_link_libraries(msUnitTest PRIVATE mqttsrc)
add_test(
    NAME msUnitTest
    COMMAND msUnitTest
        "${CMAKE_CURRENT_SOURCE_DIR}/../conf/mqttsrc.example.json"
        "${CMAKE_CURRENT_SOURCE_DIR}/../conf/mqttsrc.rules.example.json"
)

# Dry-run binary with link-time taos stubs. Used by fake_mqtt_broker.py for
# end-to-end protocol tests without a running TDengine cluster.
add_executable(taosmqttsrcDryRun ../src/msMain.c msTaosStub.c)
target_include_directories(taosmqttsrcDryRun PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    "${TD_SOURCE_DIR}/include/client"
)
target_link_libraries(taosmqttsrcDryRun PRIVATE mqttsrc)
