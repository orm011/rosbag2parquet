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
                    int buffer_rows,
                    const parquet::schema::NodeVector& fields) ;

        std::string tablename;
        std::string filename;
        std::shared_ptr<parquet::schema::GroupNode> parquet_schema;
        std::shared_ptr<arrow::io::FileOutputStream> out_file;
        std::shared_ptr<parquet::ParquetFileWriter> file_writer;

        int buffer_rows;
        int total_rows = 0;
        int rows_since_last_reset = 0;

        using column_buffer_t = std::pair<std::vector<char>, std::vector<char>>;
        std::vector<column_buffer_t> columns;

        void EmitCreateStatement(std::ostream& out) ;
        void FlushBuffers();
        void Close();
        void updateCountMaybeFlush();


};


#endif //ROSBAG2PARQUET_TABLEBUFFER_H
