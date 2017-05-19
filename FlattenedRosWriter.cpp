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
                       const string& dirname, bool verbose)
            : m_bagname(bag.getFileName()), m_dirname(dirname),
              m_loadscript(dirname + "/vertica_load_tables.sql"),
              m_verbose(verbose)
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

        m_streamtable = TableBuffer(m_dirname, "Messages", FLAGS_rows_per_group, parquet_fields, m_verbose);
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


        m_connectiontable = TableBuffer(m_dirname, "Connections", FLAGS_rows_per_group, parquet_fields, m_verbose);
        m_connectiontable.EmitCreateStatement(m_loadscript);
    }

    // only works for simple types
    void InsertData(int bufno, size_t len, const char* data, TableBuffer* buf){
        assert(data);
        assert(len < (1UL<<31)); // for things coming out as strings
        auto len32 = (uint32_t) len;

        // check the widened version is the same
        assert(len32 == len);

        auto & vec = buf->columns[bufno].first;
        vec.insert(vec.end(),
                   (const char*)&len32,
                   (const char*)&len32 + sizeof(len32)
        );

        // then, val is assumed to be a length
        auto &datavec = buf->columns[bufno].second;
        datavec.insert(datavec.end(), data, data + len);
    }

    template <typename T>
    void InsertScalar(int bufno, T val, TableBuffer* buf){
        // assert this is a struct of some kind at most?
        static_assert(sizeof(T) == 4 || sizeof(T) == 8, "make sure this works for more complex types before removing");
        auto & vec = buf->columns[bufno].first;
        vec.insert(vec.end(),
                   (const char*)&val,
                   (const char*)&val + sizeof(T)
        );
    }

    int FlattenedRosWriter::getConnectionId(const rosbag::MessageInstance& msg) const {
        auto f = m_conns_by_header.find(msg.getConnectionHeader().get());
        assert (f!=m_conns_by_header.end());
        // assert(f->second.second->datatype.size());
        // TODO: at test time, figure out some way of making a valid header
        return f->second.second->id;
    }

    void FlattenedRosWriter::RecordMessageMetadata(const rosbag::MessageInstance &msg){
        // records connection metadata into the connection table if needed
        // records stream metadata into the streamtable
        // TODO: we should add a file ID to all entries.
        // but which kind of ID should we use?
        // options:
        // UUID: long string. will cause issues with any database not using dictionary encoding, hard to use for humans.
        // on the other hand, parquet will be okay with it. and one can always choose not to load it into a db, using
        // a local id scheme for that if one wishes. So the main argument then is just whether a human readable one is
        // better.
        //
        // timestamp start  timestamp end: has some use.
        // path/filename? filenames can be different (eg for the same file). it would be helpful if humans can reason
        // about it

        InsertScalar(0, m_seqno, &m_streamtable);

        // TODO: should probably store ros timestamp as one parquet int96 column
        // also means we should modify the ros::time parser to do the same.
        // TODO: we should also probably store the bag timestamp in the per-type table itself
        // as sometimes messages either have no timestamp, or their stamp is not well set.

        InsertScalar(1, msg.getTime().sec, &m_streamtable);
        InsertScalar(2, msg.getTime().nsec, &m_streamtable);
        InsertScalar(3, msg.size(), &m_streamtable);
        InsertScalar(4, getConnectionId(msg), &m_streamtable);
        m_streamtable.updateCountMaybeFlush();
    }

    void FlattenedRosWriter::RecordAllConnectionMetadata(){
        for (const auto * c: m_conns) {
            auto & conn = *c;
            InsertScalar(0, conn.id, &m_connectiontable);
            InsertData(1, conn.topic.size(), conn.topic.data(), &m_connectiontable);
            InsertData(2, conn.datatype.size(), conn.datatype.data(), &m_connectiontable);
            InsertData(3, conn.md5sum.size(), conn.md5sum.data(), &m_connectiontable);
            InsertData(4, conn.msg_def.size(), conn.msg_def.data(), &m_connectiontable);

            auto cid = conn.header->find("callerid");
            assert(cid != conn.header->end());
            InsertData(5, cid->second.size(), cid->second.data(), &m_connectiontable);

            m_connectiontable.updateCountMaybeFlush();
        }
    }

    void FlattenedRosWriter::WriteMessage(const rosbag::MessageInstance &msg){
        RecordMessageMetadata(msg);

        // prepare get raw message bytes and place them in buffer
        auto buffer_len = msg.size();
        m_buffer.clear();
        m_buffer.reserve(buffer_len);

        assert(m_buffer.size() == 0);
        assert(m_buffer.capacity() >= msg.size());
        m_buffer.resize(buffer_len);
        ros::serialization::OStream stream(m_buffer.data(), msg.size());
        msg.write(stream);

        auto & handler = GetHandler(msg);
        // insert the sequential id
        InsertScalar(0, m_seqno, &handler.output_buf);

        // handle all message specific fields
        handler.addRow(1, msg, m_buffer.data(),
                m_buffer.data() + m_buffer.size());

        // the last two columns are the connection id, and the blob in ros msg format
        // connection id is a way to locally preserve sensor id when there are multiple sensors
        // of the same type.
        // the char probably gets dictionary compressed.
        InsertScalar(handler.output_buf.columns.size() - 2, getConnectionId(msg), &handler.output_buf)  ;
        InsertData(handler.output_buf.columns.size() - 1, msg.size(), (char*)m_buffer.data(), &handler.output_buf);

        handler.output_buf.updateCountMaybeFlush();
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
                                                              FLAGS_rows_per_group, m_verbose));
            iter = m_pertype.find(msg.getDataType());
            assert(iter != m_pertype.end());

            // emit create statement to load data easily
            iter->second.output_buf.EmitCreateStatement(m_loadscript);
        }

        assert(msg.getMD5Sum() == iter->second.md5sum);
        return iter->second;
    }


