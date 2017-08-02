#ifndef ROSBAG2PARQUET_MESSAGETABLE_H
#define ROSBAG2PARQUET_MESSAGETABLE_H

#include <string>
#include <rosbag/bag.h>

#include "utils.h"
#include "TableBuffer.h"

class MessageTable {

    // all public for now. clean up later
public:
    explicit MessageTable(const std::string& rostypename,
                        const std::string& md5sum,
                        const std::string& dirname,
                        const std::string& msgdefinition,
                        bool verbose);

    enum Action {
            SKIP, // nested arrays show up on the blobs
            SAVE
    };

    const char* action_string[SAVE+1] = {"SKIP", "SAVE"};

    void addRow(int starting_pos,
                const rosbag::MessageInstance& msg,
                const uint8_t *const buffer_start,
                const uint8_t *const buffer_end);

    void handleMessage(
            int* flat_pos,
            int recursion_depth,
            Action action,
            const uint8_t **buffer,
            const uint8_t *buffer_end,
            const RosIntrospection::ROSMessage* msgdef);

    void handleBuiltin(int* flat_pos,
                       int recursion_depth,
                       Action action,
                       const uint8_t** buffer_ptr,
                       const uint8_t* buffer_end,
                       const RosIntrospection::BuiltinType  elemtype) ;

    parquet::Type::type to_parquet_type(
            RosIntrospection::BuiltinType ros_typ);

    void toParquetSchema(
            const std::string & name_prefix,
            const RosIntrospection::ROSMessage& ros_msg_type,
            parquet::schema::NodeVector* parquet_fields);

    // removes one level of array type
    static RosIntrospection::ROSType RemoveArray(const RosIntrospection::ROSType &tp);
    const RosIntrospection::ROSMessage* GetMessage(const RosIntrospection::ROSType& name) const;

    std::string rostypename;
    std::string msgdefinition;
    std::string md5sum;
    std::string clean_tp;

    bool m_verbose = false;
    RosIntrospection::ROSTypeList type_list;
    const RosIntrospection::ROSMessage* ros_message;
    TableBuffer output_buf;
};


#endif //ROSBAG2PARQUET_MESSAGETABLE_H
