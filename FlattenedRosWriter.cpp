#include <unordered_map>
#include <memory>
#include <regex>
#include <fstream>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <gflags/gflags.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <ros/time.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>

#include "FlattenedRosWriter.h"

DEFINE_int32(rows_per_group, 4000, "max rows per parquet group");

using namespace std;
using parquet::Type;
using parquet::LogicalType;
using parquet::schema::PrimitiveNode;
using parquet::schema::GroupNode;


    FlattenedRosWriter::FlattenedRosWriter(const rosbag::Bag& bag,
                       const string& dirname)
            : m_bagname(bag.getFileName()), m_dirname(dirname),
              m_loadscript(dirname + "/vertica_load_tables.sql")
    {

        rosbag::View v;
        v.addQuery(bag);
        m_conns = v.getConnections();

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

    void FlattenedRosWriter::InitLoadScript() {
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

    void FlattenedRosWriter::InitStreamTable()
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
                PrimitiveNode::Make("size", // from bagfile, not from header
                                    parquet::Repetition::REQUIRED, parquet::Type::INT32,
                                    parquet::LogicalType::UINT_32)
        );


        parquet_fields.push_back(
                PrimitiveNode::Make("connection_id", // from bagfile, not from header
                                    parquet::Repetition::REQUIRED, parquet::Type::INT32,
                                    parquet::LogicalType::UINT_32)
        );

        parquet_fields.push_back( // raw message.
                PrimitiveNode::Make("data", // from bagfile, not from header
                                    parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY,
                                    parquet::LogicalType::NONE)
        );


        m_streamtable = TableBuffer(m_dirname, "Messages", FLAGS_rows_per_group, parquet_fields);
        m_streamtable.EmitCreateStatement(m_loadscript);
    }


    void FlattenedRosWriter::InitConnectionTable(){
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


        parquet_fields.push_back(
                PrimitiveNode::Make("callerid", // from bagfile, not from header
                                    parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY,
                                    parquet::LogicalType::UTF8)
        );


        m_connectiontable = TableBuffer(m_dirname, "Connections", FLAGS_rows_per_group, parquet_fields);
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

    void FlattenedRosWriter::RecordMessageMetadata(const rosbag::MessageInstance &msg){
        // records connection metadata into the connection table if needed
        // records stream metadata into the streamtable
        InsertToBuffer(0, m_seqno, &m_streamtable);
        InsertToBuffer(1, msg.getTime().sec, &m_streamtable);
        InsertToBuffer(2, msg.getTime().nsec, &m_streamtable);
        InsertToBuffer(3, msg.size(), &m_streamtable);


        // assuming we got all connections earlier
        auto f = m_conns_by_header.find(msg.getConnectionHeader().get());
        assert (f!=m_conns_by_header.end());
        f->second.first = true;

        assert(f->second.second->datatype.size());
        InsertToBuffer(4, f->second.second->id, &m_streamtable);

        // prepare buffer to be store raw message bytes
        {
            auto buffer_len = sizeof(m_seqno) + msg.size();
            m_buffer.clear();
            m_buffer.reserve(buffer_len);
            m_buffer.insert(m_buffer.end(), (uint8_t*)&m_seqno, (uint8_t*)&m_seqno + sizeof(m_seqno));

            assert(m_buffer.capacity() - m_buffer.size() >= msg.size());

            auto pos = m_buffer.size();
            m_buffer.resize(buffer_len);
            uint8_t *buffer_raw = m_buffer.data() + pos;
            ros::serialization::OStream stream(buffer_raw, msg.size());
            msg.write(stream);
            InsertToBuffer(5, msg.size(), &m_streamtable, (char*)buffer_raw);
        }

        m_streamtable.updateCountMaybeFlush();
    }

    void FlattenedRosWriter::RecordAllConnectionMetadata(){
        for (const auto * c: m_conns) {
            auto & conn = *c;
            InsertToBuffer(0, conn.id, &m_connectiontable);
            InsertToBuffer(1, conn.topic.size(), &m_connectiontable, conn.topic.data());
            InsertToBuffer(2, conn.datatype.size(), &m_connectiontable, conn.datatype.data());
            InsertToBuffer(3, conn.md5sum.size(), &m_connectiontable, conn.md5sum.data());
            InsertToBuffer(4, conn.msg_def.size(), &m_connectiontable, conn.msg_def.data());

            auto cid = conn.header->find("callerid");
            assert(cid != conn.header->end());
            InsertToBuffer(5, cid->second.size(), &m_connectiontable, cid->second.data());

            m_connectiontable.updateCountMaybeFlush();
        }
    }

    void FlattenedRosWriter::WriteMessage(const rosbag::MessageInstance &msg){
        RecordMessageMetadata(msg);
        GetHandler(msg).addRow(msg, m_buffer.data(),
                               m_buffer.data() + m_buffer.size());
        m_seqno++;
    }

    void FlattenedRosWriter::Close() {
        RecordAllConnectionMetadata();
        m_connectiontable.Close();


        for (auto &kv : m_pertype) {
            if (kv.second.output_buf.parquet_schema->field_count()) {
                kv.second.output_buf.Close();
            }
        }

        m_streamtable.Close();



        m_loadscript << "CREATE TABLE IF NOT EXISTS Files (" << endl;
        m_loadscript << "  file_id INTEGER PRIMARY KEY DEFAULT currval(:fileseq)" << endl;
        m_loadscript << ", file_load_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP" << endl;
        m_loadscript << ", file_path VARCHAR(:max_varchar) NOT NULL" << endl;
        m_loadscript << ");" << endl << endl;
        m_loadscript << "INSERT INTO files (file_path) VALUES ( '" << m_bagname << "' );" << endl << endl;

        m_loadscript << "-- these were all the tables" << endl;
        m_loadscript << "COMMIT;" << endl;
        m_loadscript.close();
    }

    MessageTable& FlattenedRosWriter::GetHandler(const rosbag::MessageInstance &msg) {
        auto iter = m_pertype.find(msg.getDataType());

        // use parquet_schema as a proxy for overall initialization
        if (iter == m_pertype.end()) {
            // Create a ParquetFileWriter instance once
            m_pertype.emplace(msg.getDataType(), MessageTable(msg.getDataType(),
                                                          msg.getMD5Sum(),
                                                          m_dirname,
                                                          msg.getMessageDefinition(),
                                                              FLAGS_rows_per_group));
            iter = m_pertype.find(msg.getDataType());
            assert(iter != m_pertype.end());

            // emit create statement to load data easily
            iter->second.output_buf.EmitCreateStatement(m_loadscript);
        }

        assert(msg.getMD5Sum() == iter->second.md5sum);
        return iter->second;
    }


