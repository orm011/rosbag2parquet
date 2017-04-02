cmake_minimum_required(VERSION 3.5)
project(bag_reader)

# remember to source the ros env for this to find the ros symbols
find_package(rosbag_storage)
find_package(roscpp_serialization)

#note: conda parquet is built with the older C++ abi, but your local ros is likely
#built with the new one. you can build your own parquet-cpp to have new abi on both
#or you can build your ros to have old abi on both
# in the latter case, you will need to also build this project with the old abi
    add_compile_options(-std=c++1y
                    -Wall -Werror
                    -O1 -g -fno-omit-frame-pointer
                    #-fsanitize=address
                    )

include_directories(${rosbag_storage_INCLUDE_DIRS} ${roscpp_serialization_INCLUDE_DIRS})
add_executable(bag_reader.bin bag_reader.cpp)

target_link_libraries(bag_reader.bin
        ${rosbag_storage_LIBRARIES}
        ${roscpp_serialization_LIBRARIES})

include_directories(/home/orm/parquet-cpp/build/arrow_ep/src/arrow_ep-install/include/)
link_directories(/home/orm/parquet-cpp/build/arrow_ep/src/arrow_ep-install/lib/
                /usr/local/lib)

add_executable(reader_writer.bin reader-writer.cc)

#if asan debugging, link to asan as well
target_link_libraries(reader_writer.bin
                        parquet
                        arrow_io
                        arrow
                        #asan
                    )