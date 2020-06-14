#[[
################## NickDB ##################

]]

cmake_minimum_required(VERSION 3.10)

#set default compiler flags
set(CMAKE_USER_MAKE_RULES_OVERRIDE "cmake/CompilerOptions.cmake")

project(NickDB)

set(META_PROJECT_NAME        "NickDB")
set(META_PROJECT_DESCRIPTION "CMake Project for the NickDB database")
set(META_AUTHOR_ORGANIZATION "Hamish Nicholson")
set(META_AUTHOR_DOMAIN       "https://github.com/shamazo/NickDB")

set(CMAKE_C_STANDARD 11)

# verbose cmake outputs, useful for debugging CMake issues such as linking
set(CMAKE_VERBOSE_MAKEFILE OFF)
# A small module which sets the default build type to debug if a .git is present and release otherwise.
include(cmake/default-build.cmake)



# static analysis during build defaults to off
OPTION(USE_CPPCHECK OFF)
find_program(CPPCHECK cppcheck HINTS $ENV{PROGRAMFILES}/cppcheck)
if (CPPCHECK)
    message(STATUS "found cppcheck")
else()
    message(STATUS "cppcheck not found")
endif()

if (CPPCHECK AND ${USE_CPPCHECK} MATCHES "ON")
    message(STATUS CPPCHECKON)
    set(CMAKE_C_CPPCHECK "${CPPCHECK}"
            "--template=cppcheck:{file}:{line}:{severity}: cppcheck: {file}:{line} {message}"
            "--enable=performance,portability"
            "--verbose"
            "--std=c99"
            "--inline-suppr"
            )
endif()

# Tell CMake where to look for includes
include_directories(include)
message(STATUS c base flags${CMAKE_C_FLAGS})
message(STATUS c release flags${CMAKE_C_FLAGS_RELEASE})
message(STATUS c debug flags${CMAKE_C_FLAGS_DEBUG})

add_executable(client
        client.c
        utils.c)

add_executable(server
        server.c
        parse.c
        utils.c
        db_manager.c
        client_context.c
        db_operators.c
        db_persist.c
        db_index.c
        db_hashtable.c
        )

set_target_properties(client PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build)
set_target_properties(server PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build)
