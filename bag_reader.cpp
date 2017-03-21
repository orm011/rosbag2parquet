#include <rosbag/bag.h>
#include <rosbag/view.h>
#include <iostream>

using namespace std;

int main(int argc, char **argv)
{
  rosbag::Bag bag(argv[1], rosbag::bagmode::Read);
  rosbag::View view;
  view.addQuery(bag);

  long int count = 0;
  size_t sizes = 0L;
  for (const auto &msg : view) {
    count++;
    sizes+= msg.size();
    if (count < 10){
      cout << msg.getTopic() << endl;
    }
  }
  
  cout << "hello world " << count << endl;
  return 0;
}
