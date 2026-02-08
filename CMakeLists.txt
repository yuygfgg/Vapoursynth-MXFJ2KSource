cmake_minimum_required(VERSION 3.20)

project(VapourSynth-MXFJ2KSource LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(MXFJ2K_PREFER_STATIC "Prefer static libraries for dependencies when possible" ON)
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

# grokj2k is expected to be installed
pkg_check_modules(GROK REQUIRED IMPORTED_TARGET libgrokj2kcodec)

add_library(MXFJ2KSource SHARED
    lib/MXFJ2KSource.cpp
)

target_include_directories(MXFJ2KSource PRIVATE
    "${VAPOURSYNTH_INCLUDE_DIRS}"
)

target_link_libraries(MXFJ2KSource PRIVATE
    MXFpp
    PkgConfig::GROK
)
