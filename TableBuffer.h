#ifndef ROSBAG2PARQUET_TABLEBUFFER_H
#define ROSBAG2PARQUET_TABLEBUFFER_H

#include <string>
#include <vector>
#include <ostream>
#include <utility>

#include <parquet/api/schema.h>
#include <parquet/api/io.h>
#include <parquet/api/writer.h>
#include <arrow/io/file.h>


class TableBuffer {
public:
        TableBuffer() = default;

        explicit TableBuffer(const std::string& dirname, const std::string & tablename,
                    const parquet::schema::NodeVector& fields, bool verbose) ;

        std::string tablename;
        std::string filename;
        std::shared_ptr<parquet::schema::GroupNode> parquet_schema;
        std::shared_ptr<arrow::io::FileOutputStream> out_file;
        std::shared_ptr<parquet::ParquetFileWriter> file_writer;

        int total_rows = 0;
        int learned_row_size = -1;  // late initialized
        int rows_since_last_reset = 0;
        int bytes_since_last_reset = 0;
        static const int row_group_byte_limit = 255 << 20;
        // 200 MB chunks (20 datatypes => 4 GB of buffering needed in worst case)
        // 20 columns => (avg column size is 10 MB, but min may be much smaller...)
        // Actually, compression reduces these column sizes further
        bool m_verbose = false;

        using column_buffer_t = std::pair<std::vector<char>, std::vector<char>>;
        std::vector<column_buffer_t> columns;

        void FlushBuffers();
        void Close();
        void updateCountMaybeFlush();
};


#endif //ROSBAG2PARQUET_TABLEBUFFER_H
