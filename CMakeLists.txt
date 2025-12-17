cmake_minimum_required(VERSION 3.20)
project(untitled C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LWS REQUIRED libwebsockets)
find_package(Threads REQUIRED)

add_executable(untitled
        main.c
        cb_ws.c
        kraken_ws.c
        jsmn.c
)

target_include_directories(untitled PRIVATE ${LWS_INCLUDE_DIRS})
target_link_libraries(untitled PRIVATE ${LWS_LIBRARIES} Threads::Threads)
target_compile_options(untitled PRIVATE ${LWS_CFLAGS_OTHER})
