cmake_minimum_required(VERSION 3.5)
project(bag_reader)

find_package(rosbag_storage)
find_package(roscpp_serialization)

# -fsanitize=address
add_compile_options(-std=c++1y -Wall -Werror -O1 -g -fno-omit-frame-pointer)
add_definitions(-D _GLIBCXX_USE_CXX11_ABI=0)

add_executable(bag_reader.bin bag_reader.cpp)
include_directories(${rosbag_storage_INCLUDE_DIRS} ${roscpp_serialization_INCLUDE_DIRS})
target_link_libraries(bag_reader.bin ${rosbag_storage_LIBRARIES} ${roscpp_serialization_LIBRARIES})

include_directories(/home/orm/anaconda3/pkgs/parquet-cpp-1.0.0-0/include/
        /home/orm/anaconda3/pkgs/arrow-cpp-0.2.post-0/include/)
link_directories(/home/orm/anaconda3/pkgs/parquet-cpp-1.0.0-0/lib/
        /home/orm/anaconda3/pkgs/arrow-cpp-0.2.post-0/lib/)
add_executable(reader_writer.bin reader-writer.cc)
#asan
target_link_libraries(reader_writer.bin parquet arrow_io arrow)