#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <memory>
#include <regex>
#include <utility>
#include <fstream>

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

#include <gflags/gflags.h>

#include "types.h"

// TODO: deal with -Wunused warning on validator
bool validate_path(const char* flagname, const std::string& val){
    if (val.empty()){
        printf("No path provided for --%s\n", flagname);
        return false;
    }

    return true;
}

DEFINE_string(bagfile, "", "path to input bagfile");
//DEFINE_validator(bagfile, &validate_path);
DEFINE_string(outdir, "", "path to the desired output location. An output folder will be created within.");
//DEFINE_validator(outdir, &validate_path);

using namespace std;
constexpr int NUM_ROWS_PER_ROW_GROUP = 1000;
using parquet::Type;
using parquet::LogicalType;
using parquet::schema::PrimitiveNode;
using parquet::schema::GroupNode;


const char* GetVerticaType(const parquet::schema::PrimitiveNode* nd)
{

    switch(nd->physical_type()) {
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
            switch (nd->logical_type()){
                case parquet::LogicalType::UTF8:
                    return "VARCHAR(:max_varchar)";
                case parquet::LogicalType::NONE:
                    return "LONG VARBINARY(:max_long_varbinary)";
                default:
                    cerr << "warning: unknown byte array combo";
                    parquet::schema::PrintSchema(nd, cerr);
                    return "LONG VARBINARY(:max_long_varbinary)";
            }
        case parquet::Type::FLOAT:
            return "FLOAT";
        case parquet::Type::DOUBLE:
            return "DOUBLE PRECISION";
        default:
            cerr << "ERROR: no vertica type found for this parquet type"  << endl;
            parquet::schema::PrintSchema(nd, cerr);
            cerr.flush();
            assert(false);
    }

    assert(false);
    return 0;
}

template <typename T> T ReadFromBuffer(const uint8_t** buffer)
{
    // Ros seems to de-facto settle for little endian order for its int types.
    // (their wiki does not seem to specify this, however, so maybe it could up to
    // the endianness of the machine that generated the message)
    //
    // If our machine were big endian (x86 is), reinterpreting
    // would be wrong.
    //
    static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "implement portable int read");
    T destination = (*(reinterpret_cast<const T *>( *buffer )));
    *buffer += sizeof(T);
    return destination;
}



class FlattenedRosWriter {

    enum Action {
        SKIP, // used for now, to be able to load the parts of data we can parse
        SKIP_SCALAR, // act like it is a scalar, even if it is an array (hack)
        SAVE
    };

    const char* action_string[SAVE+1] = {"SKIP", "SKIP_SCALAR", "SAVE"};

    struct TableBuffer {
        TableBuffer() = default;

        TableBuffer(const string& dirname, const string & tablename,
                    const parquet::schema::NodeVector& fields) :
        tablename(tablename)
        {

            // file
            filename.append(dirname).append("/").append(tablename).append(".parquet");

            /**
             * buffer output initialization:
             * 1) need parquet schema and output path info
             * 2) check it isn't empty (legacy?)
             * 2.5) create file in right directory with parquet ending
             * 3) create parquet output file using filename
             * 4) create a file writer out of that file
             * 5) init the buffer columns to be indexable
             */
            parquet_schema = std::static_pointer_cast<parquet::schema::GroupNode>(
                    parquet::schema::GroupNode::Make(tablename,
                                                     parquet::Repetition::REQUIRED,
                                                     fields));

            cerr << "******* Parquet schema: " << endl;
            parquet::schema::PrintSchema(parquet_schema.get(), cerr);
            cerr << "***********************" << endl;

            if (parquet_schema->field_count() == 0) {
                cerr << "NOTE: current generated schema is empty... skipping this type" << endl;
                assert(0);
            }

            // Create a local file output stream instance.
            PARQUET_THROW_NOT_OK(::arrow::io::FileOutputStream::Open(filename, &out_file));

            // Add writer properties
            parquet::WriterProperties::Builder builder;
            builder.compression(parquet::Compression::SNAPPY);
            std::shared_ptr<parquet::WriterProperties> props = builder.build();

            file_writer = parquet::ParquetFileWriter::Open(
                    out_file, parquet_schema, props);

            // initialize columns vectors
            columns.resize(parquet_schema->field_count());
        }

        std::string tablename;
        std::string filename;
        std::shared_ptr<parquet::schema::GroupNode> parquet_schema;
        std::shared_ptr<arrow::io::FileOutputStream> out_file;
        std::shared_ptr<parquet::ParquetFileWriter> file_writer;
        int total_rows = 0;
        int rows_since_last_reset = 0;

        vector<pair<
                vector<char>,
                vector<char> // for variable length types
        >> columns;

        void EmitCreateStatement(ostream& out) {
            out << "CREATE TABLE IF NOT EXISTS "
                << tablename << " (" << endl;

            out << "  file_id INTEGER NOT NULL DEFAULT currval(:fileseq)" << endl;

            for (int i = 0; i < parquet_schema->field_count(); ++i){
                auto &fld = parquet_schema->field(i);
                out << ", ";
                out << fld->name() << " ";
                assert(fld->is_primitive());
                assert(!fld->is_repeated());
                out << GetVerticaType(
                        static_cast<parquet::schema::PrimitiveNode*>(fld.get())) << endl;
            }
            out << ");" << endl << endl;

            // emit client statement to set a variable
            // allows us to run with -v path=PATH
            out << "\\set abs_path '\\'':path:'/";
            out << boost::filesystem::path(filename).filename().native();
            out << "\\''" << endl << endl;

            out << "COPY " << tablename << "(" << endl;
            for (int i = 0; i < parquet_schema->field_count(); ++i){
                auto &fld = parquet_schema->field(i);
                if (i > 0) {
                    out << ", ";
                }
                out << fld->name() << endl;
            }
            out << ") FROM :abs_path PARQUET DIRECT NO COMMIT;" << endl << endl;
        }

        void FlushBuffers(){
            auto batch_size = rows_since_last_reset;
            if (batch_size == 0){
                return;
            }

            assert(columns.size() == (uint32_t)parquet_schema->field_count());

            auto * rg_writer = file_writer->AppendRowGroup(batch_size);
            // flush buffered data to each writer
            for (int i = 0; i < (int64_t)columns.size(); ++i) {
                using namespace parquet;

                auto pn = static_cast<const PrimitiveNode*>(parquet_schema->field(i).get());
                auto & data = columns.at(i);
                auto col_writer = rg_writer->NextColumn();
                switch(pn->physical_type()) {
                    case Type::INT32:{
                        using WriterT = TypedColumnWriter<DataType<Type::INT32>>;
                        auto data_ptr = reinterpret_cast<const WriterT::T*>(data.first.data());
                        reinterpret_cast<WriterT*>(col_writer)->WriteBatch(
                                batch_size, nullptr, nullptr, data_ptr);
                        break;
                    }
                    case Type::INT64:{
                        using WriterT = TypedColumnWriter<DataType<Type::INT64>>;
                        auto data_ptr = reinterpret_cast<const WriterT::T*>(data.first.data());
                        reinterpret_cast<WriterT*>(col_writer)->WriteBatch(batch_size, nullptr, nullptr, data_ptr);
                        break;
                    }
                    case Type::FLOAT:{
                        using WriterT = TypedColumnWriter<DataType<Type::FLOAT>>;
                        auto data_ptr = reinterpret_cast<const WriterT::T*>(data.first.data());
                        reinterpret_cast<WriterT*>(col_writer)->WriteBatch(batch_size, nullptr, nullptr, data_ptr);
                        break;
                    }
                    case Type::DOUBLE: {
                        using WriterT = TypedColumnWriter<DataType<Type::DOUBLE>>;
                        auto data_ptr = reinterpret_cast<const WriterT::T*>(data.first.data());
                        reinterpret_cast<WriterT*>(col_writer)->WriteBatch(batch_size, nullptr, nullptr, data_ptr);
                        break;
                    }
                    case Type::BYTE_ARRAY: {
                        using WriterT = TypedColumnWriter<DataType<Type::BYTE_ARRAY>>;
                        auto num_entries = data.first.size()/sizeof(uint32_t);
                        vector<parquet::ByteArray> bas;
                        bas.reserve(num_entries);

                        // we have the lengths on the first vector (each size 4 bytes)
                        // then the bytes on the second.
                        // we need to translate this to a {len, ptr} structure.
                        // the len is read off the first vector
                        // the ptr is the offset in the second
                        uint32_t offset_first = 0;
                        uint32_t offset_second = 0;
                        for (int j = 0; j < (int64_t)num_entries; ++j){
                            assert(offset_first <= data.first.size());
                            auto len = *reinterpret_cast<uint32_t*>(data.first.data() + offset_first);
                            assert(offset_second + len <= data.second.size());

                            auto ptr = data.second.data() + offset_second;
                            bas.push_back(parquet::ByteArray(len, reinterpret_cast<const uint8_t*>(ptr)));

                            offset_first += sizeof(uint32_t);
                            offset_second += len;
                        }

                        // must've read everything out
                        assert(offset_first == data.first.size());
                        assert(offset_second == data.second.size());

                        reinterpret_cast<WriterT*>(col_writer)->WriteBatch(batch_size, nullptr, nullptr, bas.data());
                        break;
                    }
                    case Type::BOOLEAN: {
                        using WriterT = TypedColumnWriter<DataType<Type::BOOLEAN>>;
                        auto data_ptr = reinterpret_cast<const WriterT::T*>(data.first.data());
                        reinterpret_cast<WriterT*>(col_writer)->WriteBatch(batch_size, nullptr, nullptr, data_ptr);
                        break;
                    }
                    default:
                        assert(false && "not expecting this type after ros conversion");
                }
                col_writer->Close();
                data.first.clear();
                data.second.clear();
            }

            // update auxiliary classes
            rows_since_last_reset = 0;
            rg_writer->Close();
        }

        void Close() {
            FlushBuffers();
            file_writer->Close();
            out_file->Close();
        }

        void updateCountMaybeFlush(){
            // update counts
            rows_since_last_reset +=1;
            total_rows += 1;

            // check for batch size
            if (rows_since_last_reset == NUM_ROWS_PER_ROW_GROUP){
                FlushBuffers();
            }
        }
    };


    struct MsgTable  {

        void addRow(const rosbag::MessageInstance& msg,
                    const uint8_t *const buffer_start,
                    const uint8_t *const buffer_end)
        {
            // check if the pointers match:
            // msg.getConnectionHeader().get()

            // push raw bytes onto column buffers
            int pos = 0;
            auto buffer = buffer_start;

            handleBuiltin(&pos, 0, SAVE, &buffer, buffer_end, RosIntrospection::INT64);
            //handleBuiltin(MsgTable, &pos, 0, SAVE, &buffer, buffer_end, RosIntrospection::STRING);
            //handleBuiltin(MsgTable, &pos, 0, SAVE, &buffer, buffer_end, RosIntrospection::STRING);
            handleMessage(&pos, 0, SAVE, &buffer, buffer_end, this->ros_message);

            // all fields saved
            assert(pos == this->output_buf.parquet_schema->field_count());
            // all bytes seen
            assert(buffer == buffer_end);
            this->output_buf.updateCountMaybeFlush();
        }

        void handleMessage(
                int* flat_pos,
                int recursion_depth,
                Action action,
                const uint8_t **buffer,
                const uint8_t *buffer_end,
                const RosIntrospection::ROSMessage* msgdef) {
            assert(*buffer < buffer_end);

//        cout << string(recursion_depth, ' ') << action_string[action]
//             << "ing " << fieldname << ":" << type << endl ;

            for (auto & f : msgdef->fields()) {
                //cout << string(recursion_depth, ' ') << "field  " << f.name()
                //     << ":" << f.type().baseName() <<  endl ;

                if (f.isConstant()) continue;

                // saving only byte buffers (uint8 arrays) right now.
                if (f.type().isArray()) {

                    // handle uint8 vararray just like a string
                    if (f.type().typeID() == RosIntrospection::BuiltinType::UINT8 && f.type().arraySize() < 0) {
                        handleBuiltin(flat_pos, recursion_depth+1, action,
                                      buffer, buffer_end, RosIntrospection::BuiltinType::STRING);
                        continue;
                    }


                    // figure out how many things to skip
                    auto rawlen = f.type().arraySize();
                    uint32_t len = 0;
                    if (rawlen >= 0) { // constant array
                        len = rawlen;
                    } else if (rawlen == -1) { // variable length array
                        len = ReadFromBuffer<uint32_t>(buffer);
                    }


                    // fixed len arrays of builtins (may save)
                    if (f.type().isBuiltin() && f.type().isArray() && f.type().arraySize() > 0) {
                        for (int i = 0; i < f.type().arraySize(); ++i) {
                            handleBuiltin(flat_pos, recursion_depth +1, action,
                                          buffer, buffer_end, f.type().typeID());
                        }
                        continue;
                    }

                    // now skip them one by one
                    for (uint32_t i = 0; i < len; ++i){
                        // convert name to scalar (so it can be looked up)
                        if (f.type().isBuiltin()){
                            handleBuiltin(flat_pos, recursion_depth+1, SKIP_SCALAR, buffer,
                                          buffer_end,f.type().typeID());
                        } else {
                            auto msg = GetMessage(f.type());
                            handleMessage(flat_pos, recursion_depth + 1, SKIP_SCALAR, buffer, buffer_end,
                                          msg);
                        }
                    }

                    continue;
                } else if (!f.type().isBuiltin()) {
                    auto msg = this->GetMessage(f.type());
                    handleMessage(flat_pos, recursion_depth + 1, action, buffer, buffer_end,
                                  msg);
                } else {
                    handleBuiltin(flat_pos, recursion_depth + 1, action, buffer, buffer_end,
                                  f.type().typeID());
                }
            }
            //cout << endl;
        }

        void handleBuiltin(int* flat_pos,
                           int recursion_depth,
                           Action action,
                           const uint8_t** buffer_ptr,
                           const uint8_t* buffer_end,
                           const RosIntrospection::BuiltinType  elemtype) {
            assert(*buffer_ptr < buffer_end);
//        cout << string(recursion_depth, ' ') << action_string[action] << "ing a " << field_name
//             << ":" << elemtype << "pos " << *flat_pos << endl;

            using RosIntrospection::BuiltinType;
            pair<vector<char>, vector<char>>* vector_out;
            if (action == SAVE){
                vector_out = &this->output_buf.columns[*flat_pos];
                assert(*flat_pos < this->output_buf.parquet_schema->field_count());
                (*flat_pos) += 1;
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
                        vector_out->first.insert(vector_out->first.end(), (char*)&tmp, (char*)&tmp+sizeof(tmp));
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
                        vector_out->first.insert(vector_out->first.end(), (char*)&tmp, (char*)&tmp+sizeof(tmp));
                    }
                    return;
                }
                case BuiltinType::UINT32:
                case BuiltinType::INT32:
                {
                    auto tmp = ReadFromBuffer<int32_t>(buffer_ptr);
                    if (action == SAVE) {
                        vector_out->first.insert(vector_out->first.end(), (char*)&tmp, (char*)&tmp+sizeof(tmp));
                    }
                    return;
                }
                case BuiltinType::FLOAT32: {
                    auto tmp = ReadFromBuffer<float>(buffer_ptr);
                    if (action == SAVE) {
                        vector_out->first.insert(vector_out->first.end(), (char*)&tmp, (char*)&tmp+sizeof(tmp));
                    }
                    return;
                }
                case BuiltinType::FLOAT64: {
                    auto tmp = ReadFromBuffer<double>(buffer_ptr);
                    if (action == SAVE) {
                        vector_out->first.insert(vector_out->first.end(), (char*)&tmp, (char*)&tmp+sizeof(tmp));
                    }
                    return;
                }
                case BuiltinType::INT64:
                case BuiltinType::UINT64:
                {
                    auto tmp = ReadFromBuffer<int64_t>(buffer_ptr);
                    if (action == SAVE) {
                        vector_out->first.insert(vector_out->first.end(), (char*)&tmp, (char*)&tmp+sizeof(tmp));
                    }
                    return;
                }
                case BuiltinType::TIME: { // 2 ints (secs/nanosecs)
                    // handle as a composite type, call recusrsively
                    auto secs = ReadFromBuffer<int32_t>(buffer_ptr);
                    auto nsecs = ReadFromBuffer<int32_t>(buffer_ptr);

                    if (action == SAVE){
                        vector_out->first.insert(vector_out->first.end(), (char*)&secs, ((char*)&secs)+sizeof(secs));

                        auto * vector_out2 = &this->output_buf.columns[*flat_pos];
                        (*flat_pos) += (action == SAVE);
                        vector_out2->first.insert(vector_out2->first.end(), (char*)&nsecs, ((char*)&nsecs)+sizeof(nsecs));
                    }
                    return;
                }
                case BuiltinType::STRING: {
                    auto len = ReadFromBuffer<uint32_t>(buffer_ptr);
                    auto begin = reinterpret_cast<char*>(&len);
                    if (action == SAVE) {
                        // for now, push lengths to first vector
                        // and push bytes to second vector.
                        // pinters into an vector get invalidated upon reallocation, so
                        // we defer getting all pointers until flush time
                        // at flush time, make an array to
                        vector_out->first.insert(vector_out->first.end(), begin, begin + sizeof(len));
                        vector_out->second.insert(vector_out->second.end(), *buffer_ptr, (*buffer_ptr) + len);
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


        parquet::Type::type to_parquet_type(
                RosIntrospection::BuiltinType ros_typ)
        {
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

            assert(false); // should never get here
            return Type::BYTE_ARRAY; // warning
        };


        void toParquetSchema(
                const std::string & name_prefix,
                const RosIntrospection::ROSMessage& ros_msg_type,
                parquet::schema::NodeVector* parquet_fields)
        {

            for (auto &f: ros_msg_type.fields()){
                if (f.isConstant()) continue; // enum values?
                if (f.type().isArray()) {

                    // uint8[] is not flattened (blobs)
                    if (f.type().isBuiltin() &&
                        f.type().typeID() == RosIntrospection::BuiltinType::UINT8) {
                        parquet_fields->push_back(
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
                            parquet_fields->push_back(
                                    PrimitiveNode::Make(
                                            name_prefix + nm + '_' + std::to_string(i),
                                            parquet::Repetition::REQUIRED,
                                            tp,
                                            LogicalType::NONE)
                            );
                        }
                    } else if (f.type().arraySize() > 0){
                        assert(false && "not handling fixed sized arrays of complex types at the moment");
                    }

                    continue;
                }


                // scalars
                if (f.type().typeID() == RosIntrospection::BuiltinType::STRING ){
                    parquet_fields->push_back(PrimitiveNode::Make(
                            name_prefix + f.name().toStdString(), parquet::Repetition::REQUIRED,
                            Type::BYTE_ARRAY, LogicalType::UTF8));
                }  else if (f.type().typeID() == RosIntrospection::TIME){
                    parquet_fields->push_back(PrimitiveNode::Make(
                            name_prefix + f.name().toStdString() + "_sec", parquet::Repetition::REQUIRED,
                            Type::INT32, LogicalType::NONE));

                    parquet_fields->push_back(PrimitiveNode::Make(
                            name_prefix + f.name().toStdString() + "_nsec", parquet::Repetition::REQUIRED,
                            Type::INT32, LogicalType::NONE));
                } else if (f.type().isBuiltin()) { // non strings
                    // timestamp is a special case of a composite built in
                    parquet_fields->push_back(PrimitiveNode::Make(
                            name_prefix + f.name().toStdString(), parquet::Repetition::REQUIRED,
                            to_parquet_type(f.type().typeID()), LogicalType::NONE));
                } else {
                    auto msg = this->GetMessage(f.type());
                    toParquetSchema(name_prefix + f.name().toStdString() + '_', *msg, parquet_fields);
                }
            }
        }

        MsgTable(const string& rostypename,
                 const string& md5sum,
                 const string& dirname,
                 const string& msgdefinition)
        : rostypename(rostypename),
          msgdefinition(msgdefinition),
          md5sum(md5sum)
        {
            clean_tp = move(regex_replace(rostypename, regex("/"), "_"));
            type_list = RosIntrospection::buildROSTypeMapFromDefinition(
                    rostypename,
                    msgdefinition);

            ros_message = &type_list[0];

            // Setup the parquet schema
            parquet::schema::NodeVector parquet_fields;

            // seqno and topic
            parquet_fields.push_back(
                    parquet::schema::PrimitiveNode::Make("seqno", // unique within bagfile
                                                         parquet::Repetition::REQUIRED,
                                                         parquet::Type::INT64));

            toParquetSchema("", *ros_message, &parquet_fields);


            cerr << "******* found type " << this->rostypename << endl;
            cerr << "******* ROS msg definition: " << this->rostypename << endl;
            stringstream defn(this->msgdefinition);
            for (string line; std::getline(defn, line);) {
                boost::trim(line);
                if (line.empty()) continue;
                if (line.compare(0, 1, "#") == 0) continue;
                if (line.compare(0, 1, "=") == 0) break; // skip derived definitions
                if (find(line.begin(), line.end(), '=') != line.end()) continue;
                cerr << "  " << line << endl;
            }

            this->output_buf = TableBuffer(dirname, clean_tp, parquet_fields);
        }

        RosIntrospection::ROSTypeList type_list;
        const RosIntrospection::ROSMessage* ros_message;

        std::string rostypename;
        std::string msgdefinition;
        std::string md5sum;
        std::string clean_tp;

        TableBuffer output_buf;

        const RosIntrospection::ROSMessage* GetMessage(
                const RosIntrospection::ROSType& tp) const {
            // TODO(orm) handle nested complex types
            auto basename_equals = [&] (const auto& msg) {
                if (!tp.isArray()) {
                    return msg.type() == tp;
                }
                else {
                    // typename has [] at the end.
                    auto cmp = memcmp(msg.type().baseName().data(), tp.baseName().data(), tp.baseName().size() - 2);
                    return cmp == 0;
                }
            };

            auto it = std::find_if(type_list.begin(),
                                   type_list.end(),
                                   basename_equals);
            assert(it != type_list.end());
            return &(*it);
        }
    };



public:

    FlattenedRosWriter(const string& bagname, const string& dirname, std::vector<const rosbag::ConnectionInfo*> conns)
            : m_bagname(bagname), m_dirname(dirname),
              m_loadscript(dirname + "/vertica_load_tables.sql"),
              m_conns(move(conns))
    {
        InitLoadScript();
        InitStreamTable();
        InitConnectionTable();

        // need: match message to its connection id
        // TODO: could just parse connection header ourselves and store that.
        // then match messages to connections via headers.
        // this works if the header is there through the whole of
        for (const auto * c : m_conns){
            auto ret = m_conns_by_header.emplace(c->header.get(), make_pair(false, c));
            // we assume headers identify connections uniquely
            (void)ret;  // use var
            assert(ret.second);
        }
    }

    void InitLoadScript() {
        m_loadscript << "CREATE SCHEMA IF NOT EXISTS :schema;" << endl;
        m_loadscript << "set search_path to :schema, public;" << endl << endl;
        m_loadscript << "-- using max var type lengths allowed by vertica before it truncates" << endl;
        m_loadscript << "\\set max_varchar 65000" << endl;
        m_loadscript << "\\set max_long_varbinary 32000000" << endl;
        m_loadscript << "--  need to pass -v fileseq=<file sequence> (create a sequence if needed)" << endl;
        m_loadscript << "\\set fileseq '\\'':fileseq'\\''" << endl;
        m_loadscript << "-- gets a unique sequence number and begins transaction" << endl;
        m_loadscript << "SELECT nextval(:fileseq);" << endl << endl;
    }

    void InitStreamTable()
        {
            using namespace parquet::schema;
            // Setup the parquet schema
            NodeVector parquet_fields;

            // seqno and topic
            parquet_fields.push_back(
                    PrimitiveNode::Make("seqno", // unique within bagfile
                                       parquet::Repetition::REQUIRED, parquet::Type::INT64)
            );

            parquet_fields.push_back(
                    PrimitiveNode::Make("time_sec", // from index, not from header
                                        parquet::Repetition::REQUIRED, parquet::Type::INT32,
                                        parquet::LogicalType::UINT_32)
            );

            parquet_fields.push_back(
                    PrimitiveNode::Make("time_nsec", // from bagfile, not from header
                                        parquet::Repetition::REQUIRED, parquet::Type::INT32,
                                        parquet::LogicalType::UINT_32)
            );

            parquet_fields.push_back(
                    PrimitiveNode::Make("connection_id", // from bagfile, not from header
                                        parquet::Repetition::REQUIRED, parquet::Type::INT32,
                                        parquet::LogicalType::UINT_32)
            );

            m_streamtable = TableBuffer(m_dirname, "Messages", parquet_fields);
            m_streamtable.EmitCreateStatement(m_loadscript);
        }


    void InitConnectionTable(){
        // see rosbag/structures.h ConnectionInfo

        using namespace parquet::schema;
        // Setup the parquet schema
        NodeVector parquet_fields;

        // seqno and topic
        parquet_fields.push_back(
                PrimitiveNode::Make("connection_id", // unique within bagfile
                                    parquet::Repetition::REQUIRED,
                                    parquet::Type::INT32, parquet::LogicalType::UINT_32)
        );

        parquet_fields.push_back(
                PrimitiveNode::Make("topic", // from index, not from header
                                    parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY,
                                    parquet::LogicalType::UTF8)
        );

        parquet_fields.push_back(
                PrimitiveNode::Make("datatype", // from bagfile, not from header
                                    parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY,
                                    parquet::LogicalType::UTF8)
        );

        parquet_fields.push_back(
                PrimitiveNode::Make("md5sum", // from bagfile, not from header
                                    parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY,
                                    parquet::LogicalType::UTF8)
        );

        parquet_fields.push_back(
                PrimitiveNode::Make("msg_def", // from bagfile, not from header
                                    parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY,
                                    parquet::LogicalType::UTF8)
        );

        m_connectiontable = TableBuffer(m_dirname, "Connections", parquet_fields);
        m_connectiontable.EmitCreateStatement(m_loadscript);
    }

    // only works for simple types
    template <typename T>
    void InsertToBuffer(int bufno, T val, TableBuffer* buf, const char* data = nullptr){
        static_assert(sizeof(T) == 4 || sizeof(T) == 8, "only for int32 or int64 types");

        if (data){
            // TODO, widen the other code to use 64 bit lengths?
            assert(val < (1UL<<31)); // for things coming out as strings
            auto len = (uint32_t) val;

            auto & vec = buf->columns[bufno].first;
            vec.insert(vec.end(),
                       (const char*)&len,
                       (const char*)&len + sizeof(len)
            );

            // then, val is assumed to be a length
            auto &datavec = buf->columns[bufno].second;
            datavec.insert(datavec.end(), data, data + val);
        } else {
            auto & vec = buf->columns[bufno].first;
            vec.insert(vec.end(),
                       (const char*)&val,
                       (const char*)&val + sizeof(T)
            );
        }
    }

    int64_t RecordMessageMetadata(const rosbag::MessageInstance &msg){
        // records connection metadata into the connection table if needed
        // records stream metadata into the streamtable
        InsertToBuffer(0, m_seqno, &m_streamtable);
        InsertToBuffer(1, msg.getTime().sec, &m_streamtable);
        InsertToBuffer(2, msg.getTime().nsec, &m_streamtable);

        // assuming we got all connections earlier
        auto f = m_conns_by_header.find(msg.getConnectionHeader().get());
        assert (f!=m_conns_by_header.end());
        f->second.first = true;

        assert(f->second.second->datatype.size());
        InsertToBuffer(3, f->second.second->id, &m_streamtable);
        m_streamtable.updateCountMaybeFlush();

        return m_seqno++;
    }

    void RecordAllConnectionMetadata(){
        for (const auto * c: m_conns) {
            auto & conn = *c;
            InsertToBuffer(0, conn.id, &m_connectiontable);
            InsertToBuffer(1, conn.topic.size(), &m_connectiontable, conn.topic.data());
            InsertToBuffer(2, conn.datatype.size(), &m_connectiontable, conn.datatype.data());
            InsertToBuffer(3, conn.md5sum.size(), &m_connectiontable, conn.md5sum.data());
            InsertToBuffer(4, conn.msg_def.size(), &m_connectiontable, conn.msg_def.data());
            m_connectiontable.updateCountMaybeFlush();
        }
    }

    void RecordMessageData(const rosbag::MessageInstance &msg){
            auto seqno = RecordMessageMetadata(msg);
            auto buffer_len = sizeof(seqno) + msg.size();

            m_buffer.clear();
            m_buffer.reserve(buffer_len);
            m_buffer.insert(m_buffer.end(), (uint8_t*)&m_seqno, (uint8_t*)&m_seqno + sizeof(m_seqno));

            assert(m_buffer.capacity() - m_buffer.size() >= msg.size());
            {
                auto pos = m_buffer.size();
                m_buffer.resize(buffer_len);
                uint8_t *buffer_raw = m_buffer.data() + pos;
                ros::serialization::OStream stream(buffer_raw, msg.size());
                msg.write(stream);
                RosIntrospection::ROSType rtype(msg.getDataType());
            }

        GetHandler(msg).addRow(msg, m_buffer.data(),
                               m_buffer.data() + m_buffer.size());
    }

    void Close() {
        RecordAllConnectionMetadata();
        m_connectiontable.Close();


        for (auto &kv : m_pertype) {
            if (kv.second.output_buf.parquet_schema->field_count()) {
                kv.second.output_buf.Close();
            }
        }

        m_streamtable.Close();



        m_loadscript << "CREATE TABLE IF NOT EXISTS files (" << endl;
        m_loadscript << "  file_id INTEGER PRIMARY KEY DEFAULT currval(:fileseq)" << endl;
        m_loadscript << ", file_load_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP" << endl;
        m_loadscript << ", file_path VARCHAR NOT NULL" << endl;
        m_loadscript << ");" << endl << endl;
        m_loadscript << "INSERT INTO files (file_path) VALUES ( '" << m_bagname << "' );" << endl << endl;

        m_loadscript << "-- these were all the tables" << endl;
        m_loadscript << "COMMIT;" << endl;
        m_loadscript.close();
    }

private:


    MsgTable& GetHandler(const rosbag::MessageInstance &msg) {
        auto iter = m_pertype.find(msg.getDataType());

        // use parquet_schema as a proxy for overall initialization
        if (iter == m_pertype.end()) {
            // Create a ParquetFileWriter instance once
            m_pertype.emplace(msg.getDataType(), MsgTable(msg.getDataType(),
                                                           msg.getMD5Sum(),
                                                           m_dirname,
                                                           msg.getMessageDefinition()));
            iter = m_pertype.find(msg.getDataType());
            assert(iter != m_pertype.end());

            // emit create statement to load data easily
            iter->second.output_buf.EmitCreateStatement(m_loadscript);
        }

        assert(msg.getMD5Sum() == iter->second.md5sum);
        return iter->second;
    }

    std::vector<uint8_t> m_buffer;
    const string m_bagname;
    uint64_t m_seqno = 0;
    const string m_dirname;
    ofstream m_loadscript;
    std::vector<const rosbag::ConnectionInfo*> m_conns;
    std::unordered_map<void*, pair<bool, const rosbag::ConnectionInfo*>> m_conns_by_header;
    std::unordered_map<std::string, MsgTable> m_pertype;
    TableBuffer m_streamtable;
    TableBuffer m_connectiontable;
};


int main(int argc, char **argv)
{
    gflags::SetUsageMessage("converts ROS bag files to a directory of parquet files. "
                                    "There is one parquet file per type found in the bag");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    rosbag::Bag bag(FLAGS_bagfile, rosbag::bagmode::Read);
    rosbag::View view;
    view.addQuery(bag);

    boost::filesystem::path opath(FLAGS_outdir);
    boost::filesystem::path ifile(FLAGS_bagfile);

    auto odir = ifile.filename();
    odir.replace_extension().concat("_parquet_dir");
    opath /= odir;

    int i = 0;

    while (boost::filesystem::exists(opath)){
        ++i;
        opath.replace_extension(to_string(i));
    }

    if(boost::filesystem::create_directory(opath))
    {
        cerr<< "creating output directory " << opath.native() << endl;
    } else {
        cerr << "ERROR: Failed to create output directory." << endl;
        exit(1);
    }

    int64_t count = 0;
    FlattenedRosWriter outputs(FLAGS_bagfile, opath.native(), view.getConnections());

    for (const auto & msg : view) {
        outputs.RecordMessageData(msg);
        count+= 1;
    }

    outputs.Close();
    fprintf(stderr, "Wrote %ld records to \n%s", count, opath.c_str());
    //cout << count << endl;
}
