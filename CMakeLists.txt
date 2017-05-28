cmake_minimum_required(VERSION 3.5)
#force outside build (avoid polluting folder)
set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_CXX_STANDARD 14)

# next few lines taken from: https://github.com/google/googletest/blob/master/googletest/README.md
# import cmake
configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)

# Download and unpack googletest at configure time
configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
if(result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
if(result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_BINARY_DIR}/googletest-src
        ${CMAKE_BINARY_DIR}/googletest-build)
# end of https://github.com/google/googletest/blob/master/googletest/README.md

project(rosbag2parquet)
# remember to source the ros env for this to find the ros symbols
find_package(rosbag_storage REQUIRED)
find_package(roscpp_serialization REQUIRED)
find_package(ros_type_introspection REQUIRED)
find_package(Boost 1.58.0 REQUIRED)
include(cmake/FindArrow.cmake)
include(cmake/FindParquet.cmake)



include_directories(${PARQUET_INCLUDE_DIR})
# PARQUET_LIBRARIES PARQUET_ARROW_LIBRARIES


#NOTE:
#other deps assumed installed
#boost filesystem
#gflags
# CPLUS_INCLUDE_PATH, LIBRARY_PATH, LD_LIBRARY_PATH respectively

#NOTE:
# conda parquet is built with the older C++ abi, but your local ros is likely
# built with the new one. you can build your own parquet-cpp to have new abi on both
# or you can build your ros to have old abi on both
# in the latter case, you will need to also build this project with the old abi

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
    add_compile_options(-O2)
endif(NOT CMAKE_BUILD_TYPE)

if (CMAKE_BUILD_TYPE STREQUAL Debug)
    add_compile_options(-O0 -ggdb)
endif()

if (CMAKE_BUILD_TYPE STREQUAL Asan)
    add_compile_options(-fsanitize=address)
    set(CMAKE_EXE_LINKER_FLAGS -lasan ${CMAKE_EXE_LINKER_FLAGS})
endif()


add_compile_options(
        -Wall -Werror
        -fno-omit-frame-pointer
        -g)

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
add_executable(parquet_example.bin parquet_example.cpp utils.h)

#if asan debugging, link to asan as well
target_link_libraries(parquet_example.bin
        ${parquet_LIBRARIES}
        )

add_executable(rosbag2parquet.bin
        FlattenedRosWriter.cpp FlattenedRosWriter.h MessageTable.cpp
        MessageTable.h TableBuffer.cpp TableBuffer.h rosbag2parquet.cpp rosbag2parquet.h main.cpp)
target_link_libraries(rosbag2parquet.bin
        gflags
        ${rosbag_LIBRARIES}
        ${parquet_LIBRARIES}
        ${ros_type_introspection_LIBRARIES}
        )

add_executable(rosbag2parquet_test
        rosbag2parquet_test.cpp FlattenedRosWriter.cpp FlattenedRosWriter.h MessageTable.cpp MessageTable.h
        TableBuffer.cpp TableBuffer.h rosbag2parquet.cpp rosbag2parquet.h)

target_link_libraries(rosbag2parquet_test gtest_main
        gflags
        ${rosbag_LIBRARIES}
        ${parquet_LIBRARIES}
        ${ros_type_introspection_LIBRARIES}
        )
add_test(NAME all_tests COMMAND rosbag2parquet_test)


# # PYTHON Bindings
# include(cmake/boost-python.cmake)
# boost_python_module(rosbag2parquetpy
#     python/rosbag2parquet_wrapper.cpp)    
# target_link_libraries(rosbag2parquetpy boost_system boost_python glog)
# install(TARGETS rosbag2parquetpy
#   PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
#   LIBRARY DESTINATION lib
#   ARCHIVE DESTINATION lib
# )
