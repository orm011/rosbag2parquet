#include <gtest/gtest.h>
#include "MessageTable.h"

#include <rosbag/view.h>
#include <sensor_msgs/Imu.h>
#include <boost/filesystem.hpp>

TEST(rosbag_example, write_read_bag){
    // example of using rosbag classes

    auto tmpbag = boost::filesystem::temp_directory_path();
    tmpbag.append("tmpbag.bag");

    ros::Time::init();


    rosbag::Bag writebag(tmpbag.native(), rosbag::bagmode::Write);

    sensor_msgs::Imu test_msg;
    test_msg.header.seq = 42;
    test_msg.header.frame_id = "test_frame";
    test_msg.header.stamp = ros::Time(1,2);

    test_msg.angular_velocity.x = 0.1;
    test_msg.orientation.w = 0.44;

    writebag.write("test_topic", ros::MessageEvent<sensor_msgs::Imu>(boost::shared_ptr<sensor_msgs::Imu>(
            new sensor_msgs::Imu(test_msg))));

    writebag.close();

    rosbag::Bag readbag(tmpbag.native(), rosbag::bagmode::Read);
    rosbag::View v;
    v.addQuery(readbag);

    GTEST_ASSERT_EQ((*v.begin()).getTopic(), "test_topic");
    GTEST_ASSERT_EQ((*v.begin()).getDataType(), "sensor_msgs/Imu");

    auto first_msg = v.begin()->instantiate<sensor_msgs::Imu>();
    GTEST_ASSERT_EQ(first_msg->header.stamp, test_msg.header.stamp);
    GTEST_ASSERT_EQ(first_msg->header.seq, test_msg.header.seq);
    GTEST_ASSERT_EQ(first_msg->header.frame_id, test_msg.header.frame_id);

    GTEST_ASSERT_EQ(first_msg->angular_velocity.x, test_msg.angular_velocity.x);
    GTEST_ASSERT_EQ(first_msg->orientation.w, test_msg.orientation.w);
}