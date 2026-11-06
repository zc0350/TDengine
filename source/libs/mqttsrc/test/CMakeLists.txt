# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

set(MS_CONF_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../conf")

# Pure logic tests: no MQTT broker and no TDengine server required.
add_executable(msUnitTest msUnitTest.c)
target_link_libraries(msUnitTest PRIVATE mqttsrc)
target_compile_definitions(msUnitTest PRIVATE
    MS_TEST_EXAMPLE_CONFIG="${CMAKE_CURRENT_SOURCE_DIR}/../conf/mqttsrc.example.json"
    MS_TEST_RULES_CONFIG="${CMAKE_CURRENT_SOURCE_DIR}/../conf/mqttsrc.rules.example.json"
)
add_test(
    NAME msUnitTest
    COMMAND msUnitTest
        "${MS_CONF_DIR}/mqttsrc.example.json"
        "${MS_CONF_DIR}/mqttsrc.rules.example.json"
)

# Real sink write path (retry / backoff / reqid de-duplication) with injectable
# link-time stubs. Keeps the write path covered even in dry-run-only CI.
add_executable(msSinkTest msSinkTest.c msTaosStub.c)
target_include_directories(msSinkTest PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    "${TD_SOURCE_DIR}/include/client"
)
target_link_libraries(msSinkTest PRIVATE mqttsrc)
add_test(NAME msSinkTest COMMAND msSinkTest)

# Dry-run binary with link-time taos stubs. Used by fake_mqtt_broker.py for
# end-to-end protocol tests without a running TDengine cluster.
add_executable(taosmqttsrcDryRun ../src/msMain.c msTaosStub.c)
target_include_directories(taosmqttsrcDryRun PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    "${TD_SOURCE_DIR}/include/client"
)
target_link_libraries(taosmqttsrcDryRun PRIVATE mqttsrc)

# Register the fake-broker e2e scenarios with CTest so CONNECT/SUBSCRIBE/
# SUBACK/PUBLISH/PUBACK, QoS1, reconnect and keepalive are actually exercised.
find_package(Python3 COMPONENTS Interpreter QUIET)
if(Python3_Interpreter_FOUND)
    set(MS_BROKER "${CMAKE_CURRENT_SOURCE_DIR}/fake_mqtt_broker.py")
    add_test(NAME msE2EDefault
        COMMAND "${Python3_EXECUTABLE}" "${MS_BROKER}"
                "$<TARGET_FILE:taosmqttsrcDryRun>"
                "${MS_CONF_DIR}/mqttsrc.example.json")
    add_test(NAME msE2EDevice
        COMMAND "${Python3_EXECUTABLE}" "${MS_BROKER}"
                "$<TARGET_FILE:taosmqttsrcDryRun>"
                "${MS_CONF_DIR}/mqttsrc.device.example.json"
                "${CMAKE_CURRENT_SOURCE_DIR}/e2e_device_scenario.json")
    add_test(NAME msE2ERules
        COMMAND "${Python3_EXECUTABLE}" "${MS_BROKER}"
                "$<TARGET_FILE:taosmqttsrcDryRun>"
                "${MS_CONF_DIR}/mqttsrc.rules.example.json"
                "${CMAKE_CURRENT_SOURCE_DIR}/e2e_rules_scenario.json")
    foreach(_e2e msE2EDefault msE2EDevice msE2ERules)
        set_tests_properties(${_e2e} PROPERTIES TIMEOUT 60)
    endforeach()
else()
    message(STATUS "Python3 not found: mqttsrc e2e tests are not registered")
endif()
