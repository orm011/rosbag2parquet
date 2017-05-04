#ifndef ROSBAG2PARQUET_FLATTENEDROSWRITER_H
#define ROSBAG2PARQUET_FLATTENEDROSWRITER_H

#include <string>
#include "MessageTable.h"
#include "TableBuffer.h"

class FlattenedRosWriter {

public:
    FlattenedRosWriter(const rosbag::Bag&,
                       const std::string& outputpath);

    void WriteMessage(const rosbag::MessageInstance &);
    void Close();

private:
    void InitConnectionTable();
    void InitStreamTable();
    void InitLoadScript();

    MessageTable& GetHandler(const rosbag::MessageInstance &msg);
    void RecordMessageMetadata(const rosbag::MessageInstance &msg);
    void RecordAllConnectionMetadata();

    std::vector<uint8_t> m_buffer;
    const std::string m_bagname;
    uint64_t m_seqno = 0;
    const std::string m_dirname;
    std::ofstream m_loadscript;
    std::vector<const rosbag::ConnectionInfo*> m_conns;
    std::unordered_map<void*, std::pair<bool, const rosbag::ConnectionInfo*>> m_conns_by_header;
    std::unordered_map<std::string, MessageTable> m_pertype;
    TableBuffer m_streamtable;
    TableBuffer m_connectiontable;
};


#endif //ROSBAG2PARQUET_FLATTENEDROSWRITER_H
