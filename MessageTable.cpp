#include <regex>

#include <boost/algorithm/string.hpp>

#include "MessageTable.h"

using namespace std;

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


void MessageTable::addRow(const rosbag::MessageInstance& msg,
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

void MessageTable::handleMessage(
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

        // arrays get skipped. SKIP_SCALAR means handle only the current scalar
        // TODO: this can go away if we use our own type descriptions
        if (f.type().isArray()) {
            // handle uint8 vararray just like a string
            if (f.type().typeID() == RosIntrospection::BuiltinType::UINT8 && f.type().arraySize() < 0) {
                handleBuiltin(flat_pos, recursion_depth+1, SKIP,
                              buffer, buffer_end, RosIntrospection::BuiltinType::STRING);
                continue;
            }


            // figure out how many things to skip
            uint32_t len = 0;
            if (f.type().arraySize() >= 0) { // constant array
                len = f.type().arraySize();
            } else { // variable length array
                len = ReadFromBuffer<uint32_t>(buffer);
            }

            // in principle, this is not necessarily a scalar, just an array of less dimensions
            auto array_elt_type = RemoveArray(f.type());
            assert(!array_elt_type.isArray()); // would need more code to handle array of arrays

            // now skip them one by one
            if (!array_elt_type.isBuiltin()) {
                auto msg = GetMessage(array_elt_type);
                for (uint32_t i = 0; i < len; ++i) {
                    handleMessage(flat_pos, recursion_depth + 1, SKIP, buffer, buffer_end, msg);
                }
            } else {
                for (uint32_t i = 0; i < len; ++i) {
                    handleBuiltin(flat_pos, recursion_depth + 1, SKIP, buffer, buffer_end,
                                  f.type().typeID());
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

void MessageTable::handleBuiltin(int* flat_pos,
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
        case BuiltinType::TIME:
        case BuiltinType::DURATION:
        {   // TODO duration is pair of uint32...
            // 2 ints (secs/nanosecs)
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


parquet::Type::type MessageTable::to_parquet_type(
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


void MessageTable::toParquetSchema(
        const std::string & name_prefix,
        const RosIntrospection::ROSMessage& ros_msg_type,
        parquet::schema::NodeVector* parquet_fields)
{

    using parquet::schema::PrimitiveNode;
    using parquet::Type;
    using parquet::LogicalType;

    for (auto &f: ros_msg_type.fields()){
        if (f.isConstant()) continue; // enum values?
        if (f.type().isArray()) {
            continue; // arrays are being skipped, handled at application level right now
        }

        // scalars
        if (f.type().typeID() == RosIntrospection::BuiltinType::STRING ){
            parquet_fields->push_back(PrimitiveNode::Make(
                    name_prefix + f.name().toStdString(), parquet::Repetition::REQUIRED,
                    Type::BYTE_ARRAY, LogicalType::UTF8));
        }  else if (f.type().typeID() == RosIntrospection::TIME
                    || f.type().typeID() == RosIntrospection::DURATION){
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

MessageTable::MessageTable(const string& rostypename,
                           const string& md5sum,
                           const string& dirname,
                           const string& msgdefinition,
                           int buffer_size)
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

    this->output_buf = TableBuffer(dirname, clean_tp, buffer_size, parquet_fields);
}


// removes one level of array type
RosIntrospection::ROSType MessageTable::RemoveArray(const RosIntrospection::ROSType &tp)
{
    if (tp.isArray()) {
        // typename has [] at the end.
        string no_brackets = tp.baseName().toStdString();
        assert(no_brackets.back() == ']');
        no_brackets.pop_back();

        if (tp.arraySize() >= 0) {
            // fixed len fields have numbers as well
            // eg [9] or [36]
            string sizstring = to_string(tp.arraySize());
            assert(no_brackets.size() > sizstring.size());
            auto pos = no_brackets.size() - sizstring.size();
            auto strend = no_brackets.substr(pos, sizstring.size());
            assert (sizstring == strend);
            no_brackets.erase(pos, sizstring.size());
        }

        assert(no_brackets.back() == '[');
        no_brackets.pop_back();

        return RosIntrospection::ROSType(no_brackets);
    }

    return tp;
}


const RosIntrospection::ROSMessage* MessageTable::GetMessage(
        const RosIntrospection::ROSType& tp) const {

    auto basename_equals = [&] (const auto& msg) {
        RosIntrospection::ROSType scalar = msg.type();
        while (scalar.isArray()){
            scalar = RemoveArray(msg.type());
        }
        return scalar == tp;
    };

    auto it = std::find_if(type_list.begin(),
                           type_list.end(),
                           basename_equals);
    assert(it != type_list.end());
    return &(*it);
}