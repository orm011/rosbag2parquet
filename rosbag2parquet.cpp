#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <memory>
#include <regex>

#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#include <ros/time.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>

#include <arrow/io/file.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/api/schema.h>

#include "types.h"

using namespace std;
constexpr int NUM_ROWS_PER_ROW_GROUP = 1;
// TODO make large number (need to implement row -> col batching for this)
constexpr size_t kBufferSize = 1<<26;
// 64MB;
using parquet::Type;
using parquet::LogicalType;
using parquet::schema::PrimitiveNode;
using parquet::schema::GroupNode;

class FlattenedRosWriter {

    using type_table_t = std::unordered_map<std::string, const RosIntrospection::ROSMessage*>;

    struct typeinfo {
        std::string filename;
        RosIntrospection::ROSTypeList type_list;
        std::string rostypename;
        type_table_t type_map;
        const RosIntrospection::ROSMessage* ros_message;
        std::shared_ptr<parquet::schema::GroupNode> parquet_schema;
        std::shared_ptr<::arrow::io::FileOutputStream> out_file;
        std::shared_ptr<parquet::ParquetFileWriter> file_writer;
        parquet::RowGroupWriter *rg_writer;
        int total_rows = 0;
        int rows_since_last_reset = 0;
        vector<vector<uint8_t>> columns;
        vector<vector<size_t>> sizes;  // for variable size stuff...
    };


const char* GetVerticaType(parquet::Type::type tp,
                           parquet::LogicalType::type lt)
{
    switch(tp) {
        case parquet::Type::INT32:
        case parquet::Type::INT64:
            // all integer types map to INTEGER,
            // a vertica signed 64 bit integer (with no -2^63+1)
            // if your data has UINT64 in the large range,
            // Also, -2^63+1 is not going to be be handled correctly.
            return "INTEGER";
        case parquet::Type::BOOLEAN:
            return "BOOLEAN";
        case parquet::Type::BYTE_ARRAY:
            switch (lt){
                case parquet::LogicalType::UTF8:
                    return "VARCHAR";
                case parquet::LogicalType::NONE:
                    return "VARBINARY";
                default:
                    cerr << "warning: unkown byte array combo: " << lt;
                    return "VARBINARY";
            }
        default:
            cerr << "ERROR: no vertica type found for this parquet type" << tp << endl;
            assert(false);
    }
}

public:


    template <typename T> T ReadFromBuffer(const uint8_t** buffer)
    {
        T destination = (*(reinterpret_cast<const T *>( *buffer )));
        *buffer += sizeof(T);
        return destination;
    }

    FlattenedRosWriter(const string& dirname) :
            m_dirname(dirname), m_createfile(dirname + "/vertica_load_tables.sql")
    {}

    enum Action {
        SKIP, // used for now, to be able to load the parts of data we can parse
        SKIP_SCALAR, // act like it is a scalar, even if it is an array (hack)
        SAVE
    };

    const char* action_string[SAVE+1] = {"SKIP", "SKIP_SCALAR", "SAVE"};

    void handleMessage(
            int recursion_depth,
            Action action,
            const type_table_t &types,
            const uint8_t **buffer,
            const std::string &type,
            parquet::RowGroupWriter *rg_writer) {
        // rg_writer only used for SAVE
        // cout << string(recursion_depth, ' ') << action_string[action] << "'ing  a " << type << endl ;
        const auto& rostype = types.at(type);

        for (auto f : rostype->fields()) {
            //cout << string(recursion_depth, ' ') << "field  " << f.name()
            //     << ":" << f.type().baseName() <<  endl ;

            if (f.isConstant()) continue;

            // saving only byte buffers (uint8 arrays) right now.
            if (f.type().isArray()) {

                // figure out how many things to skip
                auto rawlen = f.type().arraySize();
                uint32_t len = 0;
                if (rawlen >= 0) { // constant array
                    len = rawlen;
                } else if (rawlen == -1) { // variable length array
                    len = ReadFromBuffer<uint32_t>(buffer);
                }

                // TODO: deal with constant size byte arrays as a base case
                if (f.type().typeID() == RosIntrospection::BuiltinType::UINT8 && action == SAVE) {
                    parquet::ColumnWriter* writer {};
                    writer = rg_writer->NextColumn();

                    parquet::ByteArray ba(len, *buffer);
                    static_cast<parquet::ByteArrayWriter *>(writer)->WriteBatch(1, nullptr, nullptr, &ba);
                    (*buffer)+=len;
                    continue;
                }

                // fixed len arrays of builtins (may save)
                if (f.type().isBuiltin() && f.type().isArray() && f.type().arraySize() > 0) {
                    for (int i = 0; i < f.type().arraySize(); ++i) {
                        handleBuiltin(recursion_depth +1, action, buffer, f.type().typeID(), rg_writer);
                    }
                    continue;
                }


                // now skip them one by one
                for (uint32_t i = 0; i < len; ++i){
                    // convert name to scalar (so it can be looked up)
                    if (f.type().isBuiltin()){
                        handleBuiltin(recursion_depth+1, SKIP_SCALAR, buffer, f.type().typeID(), nullptr);
                    } else {
                        auto scalar_name = f.type().pkgName().toStdString() + '/' + f.type().msgName().toStdString();
                        handleMessage(recursion_depth + 1, SKIP_SCALAR, types, buffer,
                                      scalar_name, nullptr);
                    }
                }

                continue;
            } else if (!f.type().isBuiltin()) {
                handleMessage(recursion_depth + 1, action, types, buffer,
                              f.type().baseName().toStdString(), rg_writer);
            } else {
                handleBuiltin(recursion_depth + 1, action, buffer, f.type().typeID(), rg_writer);
            }
        }

        //cout << endl;
    }

    void handleBuiltin(int recursion_depth,
                       Action action,
                       const uint8_t** buffer_ptr,
                       const RosIntrospection::BuiltinType  elemtype,
                       parquet::RowGroupWriter* rg_writer) {
        // cout << string(recursion_depth, ' ') << action_string[action] << "'ing a " << elemtype << endl;

        using RosIntrospection::BuiltinType;

        parquet::ColumnWriter* writer {};

        if (action == SAVE) {
            writer = rg_writer->NextColumn();
        }

        switch (elemtype) {
            case BuiltinType::INT8:
            case BuiltinType::UINT8:
            case BuiltinType::BYTE:
            case BuiltinType::BOOL:
            {
                // parquet has no single byte type. promoting to int.
                // (can add varint for later)
                auto tmp_tmp = ReadFromBuffer<int8_t>(buffer_ptr);
                auto tmp =(int32_t) tmp_tmp;
                if (action == SAVE) {
                    static_cast<parquet::Int32Writer*>(writer)->WriteBatch(1, nullptr, nullptr, &tmp);
                }
                return;
            }
            case BuiltinType::INT16:
            case BuiltinType::UINT16:
            {
                // parquet has not two byte type. promoting to int.
                // (can add varint for later)
                auto tmp_tmp = ReadFromBuffer<int16_t>(buffer_ptr);
                auto tmp =(int32_t) tmp_tmp;
                if (action == SAVE) {
                    static_cast<parquet::Int32Writer*>(writer)->WriteBatch(1, nullptr, nullptr, &tmp);
                }
                return;
            }
            case BuiltinType::UINT32:
            case BuiltinType::INT32:
            {
                auto tmp = ReadFromBuffer<int32_t>(buffer_ptr);
                if (action == SAVE) {
                    static_cast<parquet::Int32Writer *>(writer)->WriteBatch(1, nullptr, nullptr, &tmp);
                }
                return;
            }
            case BuiltinType::FLOAT32: {
                auto tmp = ReadFromBuffer<float>(buffer_ptr);
                if (action == SAVE) {
                    static_cast<parquet::FloatWriter *>(writer)->WriteBatch(1, nullptr, nullptr, &tmp);
                }
                return;
            }
            case BuiltinType::FLOAT64: {
                auto tmp = ReadFromBuffer<double>(buffer_ptr);
                if (action == SAVE) {
                    static_cast<parquet::DoubleWriter *>(writer)->WriteBatch(1, nullptr, nullptr, &tmp);
                }
                return;
            }
            case BuiltinType::INT64:
            case BuiltinType::UINT64:
            {
                auto tmp = ReadFromBuffer<int64_t>(buffer_ptr);
                if (action == SAVE) {
                    static_cast<parquet::Int64Writer *>(writer)->WriteBatch(1, nullptr, nullptr, &tmp);
                }
                return;
            }
            case BuiltinType::TIME: { // 2 ints (secs/nanosecs)
                auto secs = ReadFromBuffer<int32_t>(buffer_ptr);
                auto nsecs = ReadFromBuffer<int32_t>(buffer_ptr);

                if (action == SAVE){
                    static_cast<parquet::Int32Writer*>(writer)->WriteBatch(1, nullptr, nullptr, &secs);
                    writer = rg_writer->NextColumn();
                    static_cast<parquet::Int32Writer*>(writer)->WriteBatch(1, nullptr, nullptr, &nsecs);
                }
                return;
            }
            case BuiltinType::STRING: {
                auto len = ReadFromBuffer<uint32_t>(buffer_ptr);
                if (action == SAVE) {
                    parquet::ByteArray ba(len, *buffer_ptr);
                    static_cast<parquet::ByteArrayWriter *>(writer)->WriteBatch(1, nullptr, nullptr, &ba);
                }
                (*buffer_ptr)+=len;
                return;
            }
            default:
                cout << "TODO: add handler for type: " << elemtype << endl;
                assert(false);
                return;
        }
    }

    void writeMsg(const rosbag::MessageInstance& msg){
            std::vector<uint8_t> buffer(msg.size());
            ros::serialization::OStream stream(buffer.data(), buffer.size());
            msg.write(stream);
            RosIntrospection::ROSType rtype(msg.getDataType());
            const uint8_t* buffer_raw = buffer.data();
            auto & typeinfo = getInfo(msg);
            if (typeinfo.parquet_schema->field_count() == 0){
                return;
            }

            if (typeinfo.rows_since_last_reset == NUM_ROWS_PER_ROW_GROUP){
                typeinfo.rows_since_last_reset = 0;
                typeinfo.rg_writer->Close();
                typeinfo.rg_writer = typeinfo.file_writer->AppendRowGroup(NUM_ROWS_PER_ROW_GROUP);
            }

            handleMessage(0,
                          SAVE,
                          typeinfo.type_map,
                          &buffer_raw,
                          msg.getDataType(),
                          typeinfo.rg_writer);
            typeinfo.rows_since_last_reset +=1;
            typeinfo.total_rows += 1;
    }

    void Close() {
        for (auto &kv : m_pertype) {
            if (kv.second.parquet_schema->field_count()) {
                kv.second.rg_writer->Close();
                kv.second.file_writer->Close();
                kv.second.out_file->Close();
            }
        }
    }

private:
    parquet::schema::NodeVector toParquetSchema(
            const std::string & name_prefix,
            const RosIntrospection::ROSMessage& ros_msg_type,
            const typeinfo & tp)
    {
        // TODO: I should use a table to map ros type to pair of Type, LogicalType.
        auto to_parquet_type = [](RosIntrospection::BuiltinType ros_typ){
            using parquet::Type;
            using namespace RosIntrospection;
            switch(ros_typ){
                case BuiltinType::BOOL:
                    return Type::BOOLEAN;
                case BuiltinType::BYTE:
                case BuiltinType::CHAR:
                case BuiltinType::UINT8:
                case BuiltinType::UINT16:
                case BuiltinType::UINT32:
                case BuiltinType::INT8:
                case BuiltinType::INT16:
                case BuiltinType::INT32:
                    return Type::INT32;
                case BuiltinType::INT64:
                case BuiltinType::UINT64:
                    return Type::INT64;
                case BuiltinType::STRING:
                    return Type::BYTE_ARRAY;
                case BuiltinType::FLOAT32:
                    return Type::FLOAT;
                case BuiltinType::FLOAT64:
                    return Type::DOUBLE;
                default:
                    assert(false);
            }

        };

        parquet::schema::NodeVector parquet_fields;
        for (auto &f: ros_msg_type.fields()){
            if (f.isConstant()) continue; // enum values?
            if (f.type().isArray()) {

                // uint8[] is not flattened (blobs)
                if (f.type().isBuiltin() &&
                        f.type().typeID() == RosIntrospection::BuiltinType::UINT8) {
                    parquet_fields.push_back(
                            PrimitiveNode::Make(
                                    name_prefix + f.name().toStdString(),
                                    parquet::Repetition::REQUIRED,
                                    Type::BYTE_ARRAY, LogicalType::NONE)
                    );
                }

                // constant sized arrays of primitive types get a column for each index?
                // TODO: make this recursive to handle fixed length arrays of any type
                if (f.type().arraySize() > 0 && f.type().isBuiltin()){
                    auto nm = f.name().toStdString();
                    auto tp = to_parquet_type(f.type().typeID());
                    for (int i = 0; i < f.type().arraySize(); ++i){
                        parquet_fields.push_back(
                                PrimitiveNode::Make(
                                        name_prefix + nm + '_' + std::to_string(i),
                                        parquet::Repetition::REQUIRED,
                                        tp,
                                        LogicalType::NONE)
                        );
                    }
                }

                continue;
            }


            // scalars
            if (f.type().typeID() == RosIntrospection::BuiltinType::STRING ){
                parquet_fields.push_back(PrimitiveNode::Make(
                        name_prefix + f.name().toStdString(), parquet::Repetition::REQUIRED,
                        Type::BYTE_ARRAY, LogicalType::UTF8));
            }  else if (f.type().typeID() == RosIntrospection::TIME){
                parquet_fields.push_back(PrimitiveNode::Make(
                        name_prefix + f.name().toStdString() + "_sec", parquet::Repetition::REQUIRED,
                        Type::INT32, LogicalType::NONE));

                parquet_fields.push_back(PrimitiveNode::Make(
                        name_prefix + f.name().toStdString() + "_nsec", parquet::Repetition::REQUIRED,
                        Type::INT32, LogicalType::NONE));
            } else if (f.type().isBuiltin()) { // non strings
                // timestamp is a special case of a composite built in
                parquet_fields.push_back(PrimitiveNode::Make(
                        name_prefix + f.name().toStdString(), parquet::Repetition::REQUIRED,
                        to_parquet_type(f.type().typeID()), LogicalType::NONE));
            } else {
                // TODO(orm) handle nested complex types
                const auto & msg = tp.type_map.at(f.type().baseName().toStdString());
                auto sub_fields = toParquetSchema(name_prefix + f.name().toStdString() + '_', *msg, tp);
                for (auto & f : sub_fields) {
                    parquet_fields.push_back(f);
                }
            }
        }

        return parquet_fields;
    }

    // inits info if not yet.
    typeinfo& getInfo(const rosbag::MessageInstance &msg) {
        // Create a ParquetFileWriter instance once
        auto clean_tp = regex_replace(msg.getDataType(), regex("/"), "_");
        auto & typeinfo = m_pertype[clean_tp];

        if (!typeinfo.parquet_schema) {
            typeinfo.type_list = RosIntrospection::buildROSTypeMapFromDefinition(
                    msg.getDataType(),
                    msg.getMessageDefinition());

            for (auto &tp : typeinfo.type_list) {
                typeinfo.type_map[tp.type().baseName().toStdString()] = &tp;
            }

            typeinfo.ros_message = typeinfo.type_map[msg.getDataType()];

            // Setup the parquet schema
            auto tmp = toParquetSchema("", *typeinfo.ros_message, typeinfo);
            typeinfo.parquet_schema = std::static_pointer_cast<parquet::schema::GroupNode>(
                    parquet::schema::GroupNode::Make(msg.getDataType(),
                                    parquet::Repetition::REQUIRED,
                                    tmp));

            cout << "CREATE TABLE " << clean_tp << "(" << endl;
            for (int i = 0; i < typeinfo.parquet_schema->field_count(); ++i){
                auto &fld = typeinfo.parquet_schema->field(i);
                cout << "  " << fld->name();
                assert(fld->is_primitive());
                auto fldptr = static_cast<parquet::schema::PrimitiveNode*>(fld.get());
                cout << GetVerticaType(fldptr->physical_type(), fldptr->logical_type()) << "," << endl;
            }
            cout << ");";

            cerr << "******* found type " << msg.getDataType() << endl;
            cerr << "******* ROS msg definition: " << msg.getDataType() << endl;
            stringstream defn(msg.getMessageDefinition());
            for (string line; std::getline(defn, line);) {
                boost::trim(line);
                if (line.empty()) continue;
                if (line.compare(0, 1, "#") == 0) continue;
                if (line.compare(0, 1, "=") == 0) break; // skip derived definitions
                if (find(line.begin(), line.end(), '=') != line.end()) continue;
                cerr << "  " << line << endl;
            }
            cerr << "******* Generated parquet schema: " << endl;
            parquet::schema::PrintSchema(typeinfo.parquet_schema.get(), cerr);
            cerr << "***********************" << endl;

            if (typeinfo.parquet_schema->field_count() == 0) {
                cerr << "NOTE: current generated schema is empty... skipping this type" << endl;
                return typeinfo;
            }

            // file
            typeinfo.filename = m_dirname + "/" + clean_tp + ".parquet";
            // Create a local file output stream instance.
            using FileClass = ::arrow::io::FileOutputStream;
            auto dnme = msg.getDataType();
            PARQUET_THROW_NOT_OK(FileClass::Open(typeinfo.filename, &typeinfo.out_file));


            // Add writer properties
            parquet::WriterProperties::Builder builder;
            builder.compression(parquet::Compression::SNAPPY);
            std::shared_ptr<parquet::WriterProperties> props = builder.build();

            typeinfo.file_writer =
                    parquet::ParquetFileWriter::Open(typeinfo.out_file, typeinfo.parquet_schema, props);
            //    Append a RowGroup with a specific number of rows.
            typeinfo.rg_writer = typeinfo.file_writer->AppendRowGroup(NUM_ROWS_PER_ROW_GROUP);
            //assert(typeinfo.parquet_schema.size() == typeinfo.ros_message->fields().size());
        }

        return typeinfo;
    }

    const string m_dirname;
    ofstream m_createfile;
    std::unordered_map<std::string, typeinfo> m_pertype;
};


int main(int argc, char **argv)
{
    const std::string ROSBAG_FILENAME = argv[1];
    rosbag::Bag bag(ROSBAG_FILENAME, rosbag::bagmode::Read);
    rosbag::View view;
    view.addQuery(bag);

    boost::filesystem::path dir(ROSBAG_FILENAME);
    auto filename = dir.filename();
    filename.replace_extension().concat("_parquet_dir");
    dir.remove_leaf() /= filename;

    cerr << "writing to directory " << dir.native() << endl;
    int64_t count = 0;

    if(boost::filesystem::create_directory(dir))
    {
        cerr<< "Directory Created: " << dir.native() << endl;
    } else {
        cerr << "ERROR: Failed to create output directory." << endl;
        exit(1);
    }

    FlattenedRosWriter outputs(dir.native());
    for (const auto & msg : view) {
        outputs.writeMsg(msg);
        count+= 1;
        // TODO: remove. only doing small tests right now
        if (count == 4000) break;
    }

    outputs.Close();
    //cout << count << endl;
}
