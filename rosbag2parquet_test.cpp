#include <string>
#include <utility>
#include <unordered_map>
#include <gtest/gtest.h>
#include <memory>

#include <rosbag/view.h>
#include <ros/datatypes.h>
#include <sensor_msgs/Imu.h>
#include <boost/filesystem.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <arrow/io/file.h>
#include <parquet/api/reader.h>

#include "rosbag2parquet.h"

using namespace std;

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


    auto connection_header = boost::make_shared<ros::M_string>();

    /*
     * Example header keys printed out from an actual connection header:
     *  topic
     *  type
     *  callerid
     *  latching
     *  md5sum
     *  message_definition
     */

    connection_header->insert({{"callerid","test_caller"},
                               {"topic", "test_topic"},
                               {"type", ros::message_traits::DataType<sensor_msgs::Imu>::value()},
                               {"md5sum", ros::message_traits::MD5Sum<sensor_msgs::Imu>::value()},
                               {"message_definition", ros::message_traits::Definition<sensor_msgs::Imu>::value()}
                              });

    writebag.write("test_topic", ros::Time(3,4), test_msg, connection_header);
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

// column reader shared ptr keeps the value alive
template <typename T, parquet::Type::type PT>
pair<shared_ptr<parquet::ColumnReader>, T> ReadValue(const unique_ptr<parquet::ParquetFileReader>& r,
            const string & column_name,
            int64_t row_num)
{

    EXPECT_LE(row_num, r->metadata()->num_rows());
    int col_index = 0;
    for (; col_index < r->metadata()->num_columns(); ++col_index){
        if (r->metadata()->schema()->Column(col_index)->name() == column_name){
            break;
        }
    }

    auto actual_phys_type = r->metadata()->schema()->Column(col_index)->physical_type();
    EXPECT_EQ(actual_phys_type, PT);

    int first_row_rg = 0;
    int rg = 0;
    while (row_num >= first_row_rg + r->RowGroup(rg)->metadata()->num_rows()) {
        first_row_rg += r->RowGroup(rg)->metadata()->num_rows();
        rg += 1;
    }

    auto rgreader = r->RowGroup(rg);
    auto col_reader = rgreader->Column(col_index);
    auto typed_reader = static_cast<parquet::TypedColumnReader<parquet::DataType<PT>>*>(
            col_reader.get()
    );

    typed_reader->Skip(row_num - first_row_rg);

    T out;
    int64_t count;
    typed_reader->ReadBatch(1, nullptr, nullptr, &out, &count);
    EXPECT_EQ(count, 1);

    // values are kept alive by row group
    return {col_reader, out};
}

unique_ptr<parquet::ParquetFileReader>
checkAndGetTable(boost::filesystem::path dir_path,
                 const string& table_name,
                 vector<pair<string, parquet::Type::type>> expected_columns,
                 int64_t expected_num_rows)
{

    EXPECT_TRUE(boost::filesystem::exists(dir_path));

    dir_path.append(table_name);
    dir_path.replace_extension(".parquet");

    EXPECT_TRUE(boost::filesystem::exists(dir_path));

    unique_ptr<parquet::ParquetFileReader> table =
            parquet::ParquetFileReader::OpenFile(dir_path.native(), false);

    // for now, we just check that the expected columns exist, and have the given physical types
    // we don't care if there are more columns
    const auto *schema = table->metadata()->schema();

    EXPECT_EQ(table->metadata()->num_rows(), expected_num_rows);
    EXPECT_EQ(schema->name(), table_name);

    unordered_map<string, parquet::Type::type> existing;
    for (int i = 0; i < schema->num_columns(); ++i){
        auto res = existing.insert({schema->Column(i)->name(), schema->Column(i)->physical_type()});
        EXPECT_TRUE(res.second);
    }

    for (auto &pr : expected_columns){
        auto it = existing.find(pr.first);
        EXPECT_NE(it, existing.end());
        EXPECT_EQ(it->second, pr.second);
    }

    return table;
}

TEST(rosbag2parquet, two_messages_test){
    // sanity check over a bag with two entries
    // (two instead of one, to check seqno moves forward)
    // it may make more sense to write the tests in python.
    // but both parquet and ros have python bindings, we just need to add one for our one function.

    auto tmp_bagfile = boost::filesystem::temp_directory_path();
    tmp_bagfile.append("tmp_bagfile.bag");

    sensor_msgs::Imu test_msg;
    test_msg.header.seq = 42;
    test_msg.header.frame_id = "test_frame";
    test_msg.header.stamp = ros::Time(1, 2);
    test_msg.angular_velocity.x = 0.1;
    test_msg.orientation.w = 0.44;
    auto bag_stamp_0 = ros::Time(3,4);
    auto bag_stamp_1 = ros::Time(5,6);

    {
        ros::Time::init();
        rosbag::Bag test_bag(tmp_bagfile.native(), rosbag::bagmode::Write);

        auto event =  ros::MessageEvent<sensor_msgs::Imu>(boost::shared_ptr<sensor_msgs::Imu>(
                new sensor_msgs::Imu(test_msg)));

        auto connection_header = boost::make_shared<ros::M_string>();
        connection_header->insert(
                {{"callerid","test_caller"},
                 {"topic", "test_topic"},
                 {"type", ros::message_traits::DataType<sensor_msgs::Imu>::value()},
                 {"md5sum", ros::message_traits::MD5Sum<sensor_msgs::Imu>::value()},
                 {"message_definition", ros::message_traits::Definition<sensor_msgs::Imu>::value()}
                });

        test_bag.write("test_topic", bag_stamp_0, test_msg, connection_header);
        test_bag.write("test_topic", bag_stamp_1, test_msg, connection_header);
        test_bag.close();
    }

    // filename
    // Create a ParquetReader instance
    auto tmpparquet = boost::filesystem::temp_directory_path();
    rosbag2parquet(tmp_bagfile.native(), tmpparquet.native());

    int32_t messages_connection_id = -1;
    int32_t messages_seqno = -1;

    { // messages table
        auto parquet_reader = checkAndGetTable(tmpparquet, "Messages",
                                               {{"seqno", parquet::Type::INT64},
                                                {"connection_id", parquet::Type::INT32}},
                                               2);

        auto seqno0 = ReadValue<int64_t, parquet::Type::INT64>(parquet_reader, "seqno", 0);
        ASSERT_EQ(0, seqno0.second);

        auto seqno = ReadValue<int64_t, parquet::Type::INT64>(parquet_reader, "seqno", 1);
        ASSERT_EQ(1, seqno.second);
        messages_seqno = seqno.second;

        auto connection_id = ReadValue<int32_t, parquet::Type::INT32>(parquet_reader, "connection_id", 1);
        ASSERT_GE(connection_id.second, 0);
        messages_connection_id = connection_id.second;

        auto time_sec = ReadValue<int32_t, parquet::Type::INT32>(parquet_reader, "time_sec", 1);
        auto time_nsec = ReadValue<int32_t, parquet::Type::INT32>(parquet_reader, "time_nsec", 1);
        ASSERT_EQ(ros::Time(time_sec.second, time_nsec.second), bag_stamp_1);
    }

    {  // connections table
        auto parquet_reader = checkAndGetTable(tmpparquet, "Connections",
                                               {{"connection_id", parquet::Type::INT32}}, 1);

        auto connection_id_out = ReadValue<int32_t, parquet::Type::INT32>(parquet_reader, "connection_id", 0);
        ASSERT_EQ(messages_connection_id, connection_id_out.second);

        auto ba = ReadValue<parquet::ByteArray, parquet::Type::BYTE_ARRAY>(parquet_reader, "md5sum", 0);
        string actual((char*)ba.second.ptr, (size_t)ba.second.len);
        string expected(ros::message_traits::MD5Sum<sensor_msgs::Imu>::value());
        ASSERT_EQ(expected, actual);

        auto msg_def = ReadValue<parquet::ByteArray, parquet::Type::BYTE_ARRAY>(parquet_reader, "msg_def", 0);
        string actual_def((char*)ba.second.ptr, (size_t)ba.second.len);
        string expected_def(ros::message_traits::Definition<sensor_msgs::Imu>::value());
        ASSERT_EQ(expected, actual);
    }

    {   // actual imu message
        // TODO: for now, some redundant checks for column and type.
        auto parquet_reader = checkAndGetTable(tmpparquet, "sensor_msgs_Imu",
                                               {{"connection_id", parquet::Type::INT32},
                                                {"header_seq", parquet::Type::INT32},
                                                {"data", parquet::Type::BYTE_ARRAY}
                                               },
                                               2);

        auto seqno0 = ReadValue<int64_t, parquet::Type::INT64>(parquet_reader, "seqno", 0);
        ASSERT_EQ(0, seqno0.second);

        auto seqno_out = ReadValue<int64_t, parquet::Type::INT64>(parquet_reader, "seqno", 1);
        ASSERT_EQ(messages_seqno, seqno_out.second);

        { // test blob can be successfully deserialized and has same values
            auto data_out = ReadValue<parquet::ByteArray, parquet::Type::BYTE_ARRAY>(parquet_reader, "data", 1);
            auto data_copy = make_unique<uint8_t[]>(data_out.second.len);
            memcpy(data_copy.get(), data_out.second.ptr, data_out.second.len);
            ros::SerializedMessage msg(boost::shared_array<uint8_t>(data_copy.release()), (size_t)data_out.second.len);
            sensor_msgs::Imu deser{};
            ros::serialization::deserializeMessage(msg, deser);

            ASSERT_EQ(deser.header.frame_id, test_msg.header.frame_id);
            ASSERT_EQ(deser.header.stamp, test_msg.header.stamp);
            ASSERT_EQ(42U, deser.header.seq);
            ASSERT_EQ(deser.angular_velocity.x, test_msg.angular_velocity.x);
            ASSERT_EQ(deser.orientation.w, test_msg.orientation.w);
        }

        auto connection_id0 = ReadValue<int32_t, parquet::Type::INT32>(parquet_reader, "connection_id", 0);
        ASSERT_EQ(messages_connection_id, connection_id0.second);

        auto connection_id_out = ReadValue<int32_t, parquet::Type::INT32>(parquet_reader, "connection_id", 1);
        ASSERT_EQ(messages_connection_id, connection_id_out.second);

        auto header_seq = ReadValue<int32_t, parquet::Type::INT32>(parquet_reader, "header_seq", 1);
        ASSERT_EQ((uint32_t)header_seq.second, test_msg.header.seq);

        auto header_frame_id = ReadValue<parquet::ByteArray, parquet::Type::BYTE_ARRAY>(
                parquet_reader, "header_frame_id", 1);
        string actual_frame((char*)header_frame_id.second.ptr, (size_t)header_frame_id.second.len);
        ASSERT_EQ(actual_frame, test_msg.header.frame_id);

        auto header_stamp_sec = ReadValue<int32_t, parquet::Type::INT32>(
                parquet_reader, "header_stamp_sec", 1);

        auto header_stamp_nsec = ReadValue<int32_t, parquet::Type::INT32>(
                parquet_reader, "header_stamp_nsec", 1);

        ASSERT_EQ(ros::Time(header_stamp_sec.second, header_stamp_nsec.second),
                        test_msg.header.stamp);

        auto orientation_w = ReadValue<double, parquet::Type::DOUBLE>(parquet_reader, "orientation_w", 1);
        ASSERT_EQ(orientation_w.second, test_msg.orientation.w);
    }
}