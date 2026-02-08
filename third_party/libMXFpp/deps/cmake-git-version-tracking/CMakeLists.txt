cmake_minimum_required(VERSION 3.12)

project(${GIT_VERSION_PROJECT_PREFIX}git_version
    LANGUAGES C)

# Apply the GIT_VERSION_PROJECT_PREFIX to the function names
configure_file(git.h.in git.h)

# Define the two required variables before including
# the source code for watching a git repository.
set(PRE_CONFIGURE_FILE "git.c.in")
set(POST_CONFIGURE_FILE "${CMAKE_CURRENT_BINARY_DIR}/git.c")
include(git_watcher.cmake)

# Create a library out of the compiled post-configure file.
#
# Note that the include is a system include. This was done
# so downstream projects don't suffer from warnings on a
# 3rdparty library.
add_library(${PROJECT_NAME} OBJECT ${POST_CONFIGURE_FILE})
target_include_directories(${PROJECT_NAME} SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
add_dependencies(${PROJECT_NAME} ${GIT_VERSION_PROJECT_PREFIX}check_git)

# The C99 standard is only required because we're using <stdbool.h>.
# This could be removed if it's a problem for users, but would require the
# cmake configure() commands to translate true/false literals to 1/0.
set_property(TARGET ${PROJECT_NAME} PROPERTY C_STANDARD 99)
