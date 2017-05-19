#include <iostream>
#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string.hpp>

#include <ros/time.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>

#include "FlattenedRosWriter.h"
#include "rosbag2parquet.h"

using namespace std;

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


info rosbag2parquet(const std::string& bagfile, const std::string& opath,
                    int max_mbs, bool verbose)
{
    rosbag::Bag bag(bagfile, rosbag::bagmode::Read);
    rosbag::View view;
    view.addQuery(bag);

    FlattenedRosWriter outputs(bag, opath, verbose);

    info result {};
    result.bagname = bag.getFileName();
    for (const auto & msg : view) {
        outputs.WriteMessage(msg);
        result.count+= 1;
        result.size += msg.size();
        if (max_mbs > 0 && (result.size >> 20) >= max_mbs){
            break;
        }
    }

    outputs.Close();
    return result;
}