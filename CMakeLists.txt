cmake_minimum_required(VERSION 3.5)

#force outside build (avoid polluting folder)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

project(rosbag2parquet)
# remember to source the ros env for this to find the ros symbols
find_package(rosbag_storage REQUIRED)
find_package(roscpp_serialization REQUIRED)
find_package(ros_type_introspection REQUIRED)
#NOTE:
# place arrow/parquet include/lib dirs in
# CPLUS_INCLUDE_PATH, LIBRARY_PATH, LD_LIBRARY_PATH respectively

#note: conda parquet is built with the older C++ abi, but your local ros is likely
#built with the new one. you can build your own parquet-cpp to have new abi on both
#or you can build your ros to have old abi on both
# in the latter case, you will need to also build this project with the old abi
    add_compile_options(-std=c++1y
                    -Wall -Werror
                    -fno-omit-frame-pointer -g
                    #-O2
                    -O0 -ggdb
                    #-fsanitize=address
                    )

include_directories(
        ${rosbag_storage_INCLUDE_DIRS}
        ${roscpp_serialization_INCLUDE_DIRS}
        ${ros_type_introspection_INCLUDE_DIRS}
)
add_executable(rosbag_example.bin rosbag_example.cpp)

set(rosbag_LIBRARIES
        ${rosbag_storage_LIBRARIES}
        ${roscpp_serialization_LIBRARIES})

target_link_libraries(rosbag_example.bin
        ${rosbag_LIBRARIES}
)


set(parquet_LIBRARIES parquet arrow arrow_io)
add_executable(parquet_example.bin parquet_example.cpp types.h)

#if asan debugging, link to asan as well
target_link_libraries(parquet_example.bin
                    ${parquet_LIBRARIES}
                        #asan
        )

add_executable(rosbag2parquet.bin rosbag2parquet.cpp)
target_link_libraries(rosbag2parquet.bin
        ${rosbag_LIBRARIES}
        ${parquet_LIBRARIES}
        ${ros_type_introspection_LIBRARIES})
