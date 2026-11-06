
INCLUDE(ExternalProject)

IF(CMAKE_SYSTEM_NAME MATCHES "Windows")
    # NOTE: libargp moved to cmake/externals.cmake
    add_subdirectory(emfisis.physics.uiowa.edu/Software/C/libargp)

    IF (TOOLS_BUILD_TYPE MATCHES "Debug")
        SET(BUILD_TYPE "-DCMAKE_BUILD_TYPE=Debug")
    ELSE ()
        SET(BUILD_TYPE "")
    ENDIF ()
ENDIF()
# source/libs/mqttsrc may have already defined this target (source/ is added
# before tools/ by the top-level CMakeLists).
IF (NOT TARGET toolscJson)
    ADD_SUBDIRECTORY(toolscJson)
ENDIF()
