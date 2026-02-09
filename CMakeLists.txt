cmake_minimum_required(VERSION 3.20)

project(VapourSynth-MXFJ2KSource LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(MXFJ2K_PREFER_STATIC "Prefer static libraries for dependencies when possible" ON)
option(USE_LTO "Enable link time optimization" ON)

if(MXFJ2K_PREFER_STATIC)
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)

    set(CMAKE_POSITION_INDEPENDENT_CODE ON)

    if(NOT WIN32)
        list(REMOVE_ITEM CMAKE_FIND_LIBRARY_SUFFIXES ".a")
        list(PREPEND CMAKE_FIND_LIBRARY_SUFFIXES ".a")
    endif()

    set(PKG_CONFIG_USE_STATIC_LIBS ON)

    if(MSVC)
        cmake_policy(SET CMP0091 NEW)
        if(NOT DEFINED CMAKE_MSVC_RUNTIME_LIBRARY)
            set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>" CACHE STRING "")
        endif()
    endif()
endif()

cmake_policy(SET CMP0069 NEW)
if(USE_LTO)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# libMXF/libMXFpp as in-repo dependencies.
set(LIBMXF_BUILD_LIB_ONLY ON CACHE BOOL "" FORCE)
set(LIBMXF_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(LIBMXF_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(LIBMXF_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(LIBMXF_BUILD_MXFDUMP OFF CACHE BOOL "" FORCE)

set(LIBMXFPP_BUILD_LIB_ONLY ON CACHE BOOL "" FORCE)
set(LIBMXFPP_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(LIBMXFPP_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(LIBMXFPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(LIBMXFPP_BUILD_LIBMXF_LIB OFF CACHE BOOL "" FORCE)

add_subdirectory(third_party/libMXFpp)

find_package(PkgConfig REQUIRED)
pkg_check_modules(VAPOURSYNTH REQUIRED vapoursynth)

option(MXFJ2K_USE_BUNDLED_GROK "Link against a locally built Grok) instead of pkg-config" OFF)
set(MXFJ2K_GROK_SOURCE_DIR "" CACHE PATH "Path to grok source tree")
set(MXFJ2K_GROK_BUILD_DIR "" CACHE PATH "Path to grok build tree")

if(MXFJ2K_USE_BUNDLED_GROK)
    if(NOT MXFJ2K_GROK_SOURCE_DIR)
        set(MXFJ2K_GROK_SOURCE_DIR "${CMAKE_SOURCE_DIR}/third_party/_deps/grok-src" CACHE PATH "" FORCE)
    endif()
    if(NOT MXFJ2K_GROK_BUILD_DIR)
        set(MXFJ2K_GROK_BUILD_DIR "${CMAKE_SOURCE_DIR}/third_party/_deps/grok-build" CACHE PATH "" FORCE)
    endif()

    set(GROK_INCLUDE_DIRS
        "${MXFJ2K_GROK_SOURCE_DIR}/src/lib/core"
        "${MXFJ2K_GROK_BUILD_DIR}/src/lib/core"
    )

    set(GROK_LIBRARY_DIR "${MXFJ2K_GROK_BUILD_DIR}/bin")
    set(GROK_LIBRARIES
        "${GROK_LIBRARY_DIR}/libgrokj2kcodec.a"
        "${GROK_LIBRARY_DIR}/libgrokj2k.a"
        "${GROK_LIBRARY_DIR}/libhwy.a"
        "${GROK_LIBRARY_DIR}/liblcms2.a"
    )

    foreach(path IN LISTS GROK_LIBRARIES)
        if(NOT EXISTS "${path}")
            message(FATAL_ERROR "Bundled Grok library not found: ${path}")
        endif()
    endforeach()
else()
    # grokj2k is expected to be installed
    pkg_check_modules(GROK REQUIRED IMPORTED_TARGET libgrokj2kcodec)
endif()

add_library(MXFJ2KSource SHARED
    lib/MXFJ2KSource.cpp
)

target_include_directories(MXFJ2KSource PRIVATE
    "${VAPOURSYNTH_INCLUDE_DIRS}"
)

target_link_libraries(MXFJ2KSource PRIVATE
    MXFpp
)

if(MXFJ2K_USE_BUNDLED_GROK)
    target_include_directories(MXFJ2KSource PRIVATE ${GROK_INCLUDE_DIRS})
    target_link_libraries(MXFJ2KSource PRIVATE ${GROK_LIBRARIES})
    target_compile_definitions(MXFJ2KSource PRIVATE GRK_STATIC)
else()
    target_link_libraries(MXFJ2KSource PRIVATE PkgConfig::GROK)
endif()

if(MXFJ2K_PREFER_STATIC AND WIN32 AND NOT MXFJ2K_USE_BUNDLED_GROK)
    target_compile_definitions(MXFJ2KSource PRIVATE GRK_STATIC)
endif()

if(MXFJ2K_PREFER_STATIC AND MINGW)
    target_link_options(MXFJ2KSource PRIVATE
        -static-libgcc
        -static-libstdc++
    )

    target_link_libraries(MXFJ2KSource PRIVATE
        -Wl,--whole-archive
        -l:libwinpthread.a
        -Wl,--no-whole-archive
    )
endif()
