#include "TableBuffer.h"

#include <iostream>
#include <boost/filesystem.hpp>

#include "utils.h"

using namespace std;

TableBuffer::TableBuffer(const string& dirname, const string & tablename,
                        const parquet::schema::NodeVector& fields,
                        bool verbose) :
            tablename(tablename),
            m_verbose(verbose)
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

        if (verbose) {
            cerr << "******* Parquet schema: " << endl;
            parquet::schema::PrintSchema(parquet_schema.get(), cerr);
            cerr << "***********************" << endl;
        }

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

    void TableBuffer::EmitCreateStatement(ostream& out) {
        out << "CREATE TABLE IF NOT EXISTS "
            << tablename << " (" << endl;

        out << "  ID AUTO_INCREMENT PRIMARY KEY -- redundant, but helps load data substantially faster" << endl;
        out << ", file_id INTEGER NOT NULL DEFAULT currval(:fileseq)" << endl;

        string header_keyword = "header_stamp_nsec";
        string message_keyword = "time_nsec";

        for (int i = 0; i < parquet_schema->field_count(); ++i){
            auto &fld = parquet_schema->field(i);
            out << ", ";
            out << fld->name() << " ";
            assert(fld->is_primitive());
            assert(!fld->is_repeated());
            out << GetVerticaType(
                    static_cast<parquet::schema::PrimitiveNode*>(fld.get())) << endl;

            // any table with a header will also get a timestamp
            if (memcmp(fld->name().data(), header_keyword.data(), header_keyword.size()) == 0){
                out << ", header_timestamp TIMESTAMPTZ NOT NULL default "
                        "to_timestamp(header_stamp_sec + header_stamp_nsec*1e-9)";
                out << endl;
            }

            // message table timestamp
            if (memcmp(fld->name().data(), message_keyword.data(), message_keyword.size()) == 0){
                out << ", timestamp TIMESTAMPTZ NOT NULL default "
                        "to_timestamp(time_sec + time_nsec*1e-9)";
                out << endl;
            }
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

    void TableBuffer::FlushBuffers() {
        auto batch_size = rows_since_last_reset;
        if (batch_size == 0) {
            return;
        }

        assert(columns.size() == (uint32_t)parquet_schema->field_count());

        auto * rg_writer = file_writer->AppendRowGroup(batch_size);
        // flush buffered data to each writer
        for (int i = 0; i < (int64_t)columns.size(); ++i) {
            using namespace parquet;

            auto pn = static_cast<const parquet::schema::PrimitiveNode*>(parquet_schema->field(i).get());
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
        if (learned_row_size < 0){
            learned_row_size = rows_since_last_reset;
        }

        rows_since_last_reset = 0;
        bytes_since_last_reset = 0;
        rg_writer->Close();
    }

    void TableBuffer::Close() {
        FlushBuffers();
        file_writer->Close();
        out_file->Close();
    }

    void TableBuffer::updateCountMaybeFlush(){
        // update counts
        rows_since_last_reset +=1;
        total_rows += 1;

        // check for batch size
        if (rows_since_last_reset == learned_row_size
            || (learned_row_size < 0 && bytes_since_last_reset >= row_group_byte_limit)) {
            FlushBuffers();
        }
    }