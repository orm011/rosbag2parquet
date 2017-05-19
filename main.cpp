#include <string>
#include <iostream>

#include <boost/filesystem.hpp>
#include <gflags/gflags.h>

#include "rosbag2parquet.h"

using namespace std;

// TODO: deal with -Wunused warning on validator
bool validate_path(const char* flagname, const std::string& val){
    if (val.empty()){
        printf("No path provided for --%s\n", flagname);
        return false;
    }

    return true;
}


DEFINE_string(bagfile, "", "path to input bagfile");
//DEFINE_validator(bagfile, &validate_path);
DEFINE_string(outdir, "", "path to the desired output location. An output folder will be created within.");
//DEFINE_validator(outdir, &validate_path);
DEFINE_int32(max_mbs, -1, "how much data (in MBs) to read before stopping, for testing");
DEFINE_bool(verbose, false, "print messages as program advances");


int main(int argc, char **argv)
{
    gflags::SetUsageMessage("converts ROS bag files to a directory of parquet files. "
                                    "There is one parquet file per type found in the bag");
    gflags::ParseCommandLineFlags(&argc, &argv, FLAGS_verbose);
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

    auto info = rosbag2parquet(FLAGS_bagfile, opath.native(), FLAGS_max_mbs, true);
    fprintf(stderr, "Read and parsed %ld (%ld MB uncompressed) ROS messages from %s into\n"
                    "%s\n",
            info.count, (info.size >> 20), info.bagname.c_str(), opath.native().c_str());

}
