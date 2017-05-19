#ifndef ROSBAG2PARQUET_ROSBAG2PARQUET_H
#define ROSBAG2PARQUET_ROSBAG2PARQUET_H

#include <string>

struct info {
    std::string bagname;
    int64_t count;
    int64_t size;
};


info rosbag2parquet(const std::string& bagfile, const std::string& opath,
                    int max_mbs = -1, bool verbose=false);

#endif //ROSBAG2PARQUET_ROSBAG2PARQUET_H
