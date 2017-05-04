#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <regex>
#include <fstream>

#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#include <ros/time.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>

#include <gflags/gflags.h>
#include "FlattenedRosWriter.h"

using namespace std;

// TODO: deal with -Wunused warning on validator
bool validate_path(const char* flagname, const std::string& val){
    if (val.empty()){
        printf("No path provided for --%s\n", flagname);
        return false;
    }

    return true;
}

/**
 * TODOs: (for this to be a tool for more people than just me)
 * 0) basic unit test
 * 0.1) copy ros introspection dep headers into project, ack license
 * 0.2) OR, with a bit more work, remove dep on ros introspection (just parse the type)
 * 0.3) get fastparquet to read the generated parquet files... (not sure where the problem is at the moment)
 *
 * 1) check input/output path validity before opening rosbag (want to fail quickly)
 * 2) compute aggregate statistics like rosbag info does
 *   (per connection and global: min timestamp, max timestamp, total bytes, total messages)
 *  2.1) save these to the corresponding connection and file table
 * 3) define flush based on hitting max memory usage or max tuples
 * 3.1) perhaps emit blobs as a standalone table? this would allow its buffering to be managed separately
 * 4) some of the asserts should always run (buffer checks)
 * 5) compute per msg crc? (and while we are at it: per file crc)?
 * 6) emit a header timestamp to the same global parquet table (requires adding nulls for msgs without header stamp),
 *   6.1) do not emit header time stamp to individual metadata tables
 * 7) use parquet timestamps rather than ros sec/nsec?
 *   7.1) would require checking support also at load time?
 * 8) get parquet to generate column chunk statistics on columns other than seqno (eg, both timestamps)
 * 9) the save code per message should just use a call to the convenience push functions
 * 10) there should be no need to add a fake field to the message buffer. (just call push functions)
 * 11) signed/unsigned
 * 12) rosnode to save the parquet file at least for the blob?
 */

DEFINE_string(bagfile, "", "path to input bagfile");
//DEFINE_validator(bagfile, &validate_path);
DEFINE_string(outdir, "", "path to the desired output location. An output folder will be created within.");
//DEFINE_validator(outdir, &validate_path);
DEFINE_int32(max_mbs, -1, "how much data (in MBs) to read before stopping, for testing");

int main(int argc, char **argv)
{
    gflags::SetUsageMessage("converts ROS bag files to a directory of parquet files. "
                                    "There is one parquet file per type found in the bag");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    rosbag::Bag bag(FLAGS_bagfile, rosbag::bagmode::Read);
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
    int64_t total_bytes = 0;
    FlattenedRosWriter outputs(bag, opath.native());

    rosbag::View view;
    view.addQuery(bag);
    for (const auto & msg : view) {
        outputs.WriteMessage(msg);
        count+= 1;
        total_bytes+= msg.size();
        if (FLAGS_max_mbs > 0 && (total_bytes >> 20) >= FLAGS_max_mbs){
            break;
        }
    }

    outputs.Close();
    fprintf(stderr, "Wrote %ld ROS messages (%ld MB of ROS message blobs) to \n%s\n",
            count, (total_bytes >> 20), opath.c_str());
    //cout << count << endl;
}
