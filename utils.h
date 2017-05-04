#ifndef ROSBAG2PARQUET_TYPES_H
#define ROSBAG2PARQUET_TYPES_H

#include <parquet/types.h>
#include <parquet/schema.h>
#include <iostream>
#include <cassert>

// TODO: remove dependence on ros_type_introspection
// only really need
// #include <ros/builtin_message_traits.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <ros_type_introspection/deserializer.hpp>
#pragma GCC diagnostic pop


/**
 * http://wiki.ros.org/msg
 * Based on libraries such as ros_introspection
 * like the layout is little endian, but there
 * is no description of this in the documentation
 * so, assuming this for now... (but it seems this may be up to who wrote the
 * messages?)
 * http://answers.ros.org/question/215165/what-is-the-endianness-of-ros-message-fields/
 * PrimitiveType    Serialization

    bool            unsigned 8-bit int
    int8            signed 8-bit int
    uint8           unsigned 8-bit int
    char            (deprecated) alias for uint8
    byte            (deprecated) alias for int8

    int16           signed 16-bit int
    uint16          unsigned 16-bit int

    int32           signed 32-bit int
    uint32          unsigned 32-bit int

    int64           signed 64-bit int
    uint64          unsigned 64-bit int

    float32         32-bit IEEE float
    float64         64-bit IEEE float

    string          ascii string (4)
    time            secs/nsecs unsigned 32-bit ints
    duration        secs/nsecs signed 32-bit ints

    Array type
    fixed-length    no extra serialization
    variable-length uint32 length prefix
    uint8[]         see above
    bool[]          see above
 *
 */
// general approach: by the time we write, all special cases (other than variable lengths)
// have been taken care of.
// hence, the lengths must already be done.
// need handler for buffering each special ros primitive type (string, bytearray, timestamp, elementary)
// ros type -> mem type -> (parquet type, parquet logical)
// need handler for writing each parquet type.
// elem, string, bytearray, timestamp


/*
    namespace type_table {
        template <RosIntrospection::BuiltinType RosType> struct entry;

#define ENTRY($ros_type, $ros_serial, $parquet_type, $parquet_logical)\
        template<> struct entry<$ros_type> {\
            using c_type = $ros_serial;\
            static constexpr size_t serial_byte_size = sizeof($ros_serial);\
            static constexpr parquet::Type::type parquet_type = ($parquet_type);\
            static constexpr parquet::LogicalType::type parquet_logical_type = ($parquet_logical);\
        }

        ENTRY(RosIntrospection::BOOL,  uint8_t, parquet::Type::BOOLEAN, parquet::LogicalType::NONE);

        ENTRY(RosIntrospection::INT8,  int8_t,  parquet::Type::INT32,   parquet::LogicalType::INT_8);
        ENTRY(RosIntrospection::BYTE,  int8_t,  parquet::Type::INT32,   parquet::LogicalType::INT_8);
        ENTRY(RosIntrospection::INT16, int16_t, parquet::Type::INT32,   parquet::LogicalType::INT_16);
        ENTRY(RosIntrospection::INT32, int32_t, parquet::Type::INT32,   parquet::LogicalType::INT_32);
        ENTRY(RosIntrospection::INT64, int64_t, parquet::Type::INT64, parquet::LogicalType::INT_64);

        ENTRY(RosIntrospection::UINT8, uint8_t,  parquet::Type::INT32,   parquet::LogicalType::UINT_8);
        ENTRY(RosIntrospection::CHAR,  uint8_t,  parquet::Type::INT32,   parquet::LogicalType::UINT_8);
        ENTRY(RosIntrospection::UINT16, uint16_t,  parquet::Type::INT32,   parquet::LogicalType::UINT_16);
        ENTRY(RosIntrospection::UINT32, uint32_t,  parquet::Type::INT32,   parquet::LogicalType::UINT_32);
        ENTRY(RosIntrospection::UINT64, uint64_t, parquet::Type::INT64, parquet::LogicalType::UINT_64);

        ENTRY(RosIntrospection::FLOAT32, float, parquet::Type::FLOAT, parquet::LogicalType::NONE);
        ENTRY(RosIntrospection::FLOAT64, double, parquet::Type::DOUBLE, parquet::LogicalType::NONE);
#undef ENTRY
        // strings, byte arrays and timestamps handled manually
    }
*/

inline const char* GetVerticaType(const parquet::schema::PrimitiveNode* nd)
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
                    std::cerr << "warning: unknown byte array combo";
                    parquet::schema::PrintSchema(nd, std::cerr);
                    return "LONG VARBINARY(:max_long_varbinary)";
            }
        case parquet::Type::FLOAT:
            return "FLOAT";
        case parquet::Type::DOUBLE:
            return "DOUBLE PRECISION";
        default:
        std::cerr << "ERROR: no vertica type found for this parquet type"  << std::endl;
            parquet::schema::PrintSchema(nd, std::cerr);
        std::cerr.flush();
            assert(false);
    }

    assert(false);
    return 0;
}


#endif //ROSBAG2PARQUET_TYPES_H
