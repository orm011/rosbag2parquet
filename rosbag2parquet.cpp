#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <memory>
#include <boost/shared_ptr.hpp>

#include <ros/datatypes.h>
#include <ros/time.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>

#include <arrow/io/file.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <ros_type_introspection/deserializer.hpp>
#pragma GCC diagnostic pop

using namespace std;
constexpr int NUM_ROWS_PER_ROW_GROUP = 500;
constexpr int FIXED_LENGTH = 10;

using parquet::Type;
using parquet::LogicalType;
using parquet::schema::PrimitiveNode;
using parquet::schema::GroupNode;

class FlattenedRosWriter {
public:
    FlattenedRosWriter(const string& filename) : m_filename(filename) {
        // Create a local file output stream instance.
        using FileClass = ::arrow::io::FileOutputStream;
        PARQUET_THROW_NOT_OK(FileClass::Open(m_filename, &m_out_file));
    }

    void writeMsg(const rosbag::MessageInstance& msg){
        // TODO(orm): may want to reuse per type to avoid allocs
        RosIntrospection::ROSTypeFlat tmp_container;

        std::vector<uint8_t> buffer(msg.size());
        ros::serialization::OStream stream(buffer.data(), buffer.size());
        msg.write(stream);
        RosIntrospection::ROSType rtype(msg.getDataType());
        // TODO(orm): should not reinit every time... only done once per type
        m_seen_types = RosIntrospection::buildROSTypeMapFromDefinition(msg.getDataType(), msg.getMessageDefinition());
        RosIntrospection::buildRosFlatType(m_seen_types,
                                           rtype,
                                           "",
                                           buffer.data(),
                                           &tmp_container,
        16000);

        if (msg.getDataType() == "sensor_msgs/Imu") {
            cout << msg.getDataType() << endl;
            //cout << msg.getMessageDefinition() << endl


            for (auto &kv: tmp_container.value) {
                cout << kv.first << " (value) : " << kv.second << endl;
            }

            for (auto &kv: tmp_container.name) {
                cout << kv.first << " (name) : " << kv.second.size() << endl;
            }
        }

        //auto gw = getGroupWriter(msg);
    }

private:
    std::shared_ptr<GroupNode> getSchemaForMsg(const rosbag::MessageInstance& msg){
        return nullptr;
    }

    parquet::RowGroupWriter* getGroupWriter(const rosbag::MessageInstance &msg) {
        // Setup the parquet schema
        std::shared_ptr<GroupNode> schema = getSchemaForMsg(msg);

        // Add writer properties
        parquet::WriterProperties::Builder builder;
        builder.compression(parquet::Compression::SNAPPY);
        std::shared_ptr<parquet::WriterProperties> props = builder.build();

        // Create a ParquetFileWriter instance
        std::shared_ptr<parquet::ParquetFileWriter> file_writer =
                parquet::ParquetFileWriter::Open(m_out_file, schema, props);

        //    Append a RowGroup with a specific number of rows.
        parquet::RowGroupWriter* rg_writer = m_file_writer->AppendRowGroup(NUM_ROWS_PER_ROW_GROUP);
        return rg_writer;
    }

    const string m_filename;

    // TODO(orm): make into a map of type rostype -> (outfile, file writer, group writer, columns)
    std::shared_ptr<::arrow::io::FileOutputStream> m_out_file;
    std::shared_ptr<parquet::ParquetFileWriter> m_file_writer;
    RosIntrospection::ROSTypeList m_seen_types;
};


int main(int argc, char **argv)
{
    const std::string ROSBAG_FILENAME = argv[1];
    rosbag::Bag bag(ROSBAG_FILENAME, rosbag::bagmode::Read);
    rosbag::View view;
    view.addQuery(bag);

    const std::string PARQUET_FILENAME = ROSBAG_FILENAME + ".parquet";
    int64_t count = 0;
    FlattenedRosWriter outputs(PARQUET_FILENAME);
    for (const auto & msg : view) {
        outputs.writeMsg(msg);
        count+= msg.size();
    }
    cout << count << endl;
}
