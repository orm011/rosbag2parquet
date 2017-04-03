#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <boost/shared_ptr.hpp>

#include <ros/datatypes.h>
#include <ros/time.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>

#include <arrow/io/file.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>


using namespace std;
constexpr int NUM_ROWS_PER_ROW_GROUP = 500;
constexpr int FIXED_LENGTH = 10;

using parquet::Type;
using parquet::LogicalType;
using parquet::schema::PrimitiveNode;
using parquet::schema::GroupNode;


int main(int argc, char **argv)
{
    const std::string ROSBAG_FILENAME = "/home/orm/test_data/blue_100M.bag";
    rosbag::Bag bag(ROSBAG_FILENAME, rosbag::bagmode::Read);
    rosbag::View view;
    view.addQuery(bag);

    const std::string PARQUET_FILENAME = "output.parquet";

    int64_t count = 0;
    for (const auto & msg : view) {
        count+= msg.size();
    }
    cout << count << endl;
}
