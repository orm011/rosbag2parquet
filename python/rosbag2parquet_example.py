import os
import sys
import argparse
from rosbag2parquetpy import rosbag2parquet

if __name__ == "__main__":
    
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='rosbag2parquet example')
    parser.add_argument(
        '-f', '--filename', type=str, required=True,
        help="Input bag file")
    parser.add_argument(
        '-o', '--output_dir', type=str, required=True,
        help="Output parquet directory")
    parser.add_argument(
        '--verbose', action='store_true', 
        required=False)
    args = parser.parse_args()
    
    output_dir = os.path.expanduser(args.output_dir)
    try:
        os.makedirs(output_dir)
    except Exception, e:
        print('Could not create path: {}'.format(e))
        sys.exit(1)
        
    info = rosbag2parquet(os.path.expanduser(args.filename), output_dir, max_mbs=100, verbose=args.verbose)
    print('info bagname: {}, size: {}, count: {}'.format(info.bagname, info.size, info.count))
