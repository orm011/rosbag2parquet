#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <boost/shared_ptr.hpp>


#include "ros/datatypes.h"
#include "ros/time.h"
#include "rosbag/bag.h"
#include "rosbag/view.h"

using namespace std;

int main(int argc, char **argv)
{
  rosbag::Bag bag(argv[1], rosbag::bagmode::Read);
  rosbag::View view;
  view.addQuery(bag);

  struct summary {
    size_t count = 0;
    size_t total_size = 0;
    ros::Time min_stamp = ros::TIME_MAX;
    ros::Time max_stamp = ros::TIME_MIN;
    string type_name;
    string type_md5;

    double get_freq() const {
      return count/(max_stamp - min_stamp).toSec();
    }

    double get_data_rate() const {
      return total_size/(max_stamp - min_stamp).toSec();
    }
  };

  unordered_map<string, struct summary> per_topic;
  for (const auto &msg : view) {
    auto &entry =  per_topic[msg.getTopic()];
    if (entry.count == 0){
      entry.type_name = msg.getDataType();
      entry.type_md5 = msg.getMD5Sum();
    }

    entry.count++;
    entry.total_size += msg.size();
    entry.min_stamp = min(entry.min_stamp, msg.getTime());
    entry.max_stamp = max(entry.max_stamp, msg.getTime());
  }

  unordered_map<string, struct summary> per_type;
  summary global;

  cout << "Per topic:" << endl;
  for (const auto &pr : per_topic){
    auto &entry = per_type[pr.second.type_name];
    entry.count += pr.second.count;
    entry.total_size += pr.second.total_size;
    entry.min_stamp = min(entry.min_stamp, pr.second.min_stamp);
    entry.max_stamp = max(entry.max_stamp, pr.second.max_stamp);

    global.count += pr.second.count;
    global.total_size += pr.second.total_size;
    global.min_stamp =  min(entry.min_stamp, pr.second.min_stamp);
    global.max_stamp =  max(entry.max_stamp, pr.second.max_stamp);

    cout << "topic: " << pr.first << endl;
    cout << "type: " << pr.second.type_name << endl;
    cout << "count: " << pr.second.count << endl;
    cout << "total bytes: " << pr.second.total_size << endl;
    cout << "freq: " << pr.second.get_freq() << endl;
    cout << "data rate: " << pr.second.get_data_rate() << endl;
  }
  

  return 0;
}
