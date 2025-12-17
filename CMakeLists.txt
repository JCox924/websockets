cmake_minimum_required(VERSION 3.20)
project(websockets C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_check_modules(LWS REQUIRED IMPORTED_TARGET libwebsockets)

add_executable(websockets

        cb_ws.c
        kraken_ws.c
        jsmn.c
        main.c
)

target_link_libraries(websockets PRIVATE PkgConfig::LWS)
target_link_libraries(websockets PRIVATE Threads::Threads)
target_compile_definitions(websockets PRIVATE
        _GNU_SOURCE
        _XOPEN_SOURCE=700
        _POSIX_C_SOURCE=200809L
)