# rosbag2parquet
Rosbag2parquet transforms ROS .`bag` files into 
query friendlier `.parquet` files

*Current level of functionality:*
* It handles all primitive types other than duration.
* It handles byte arrays (eg, in Compressed_Image)
* It handles fixed sized arrays of any primitive type

##Output description:
A bagfile contains message data as well as bag metadata.
We create two metadata tables (Messages and Connections)
to hold bag metadata (eg, bag timestamps) and we create 
one parquet file per message type found to store all messages
of that type. We use the message descriptions
in the bag to parse the data out, so there is no need to 
compile/link in any message definitions.

Each message on each table gets a seqno that identifies
 its original position within the bagfile.

Additionally, we are also generating a `.sql` script with all
the code needed to load the output `.parquet` files into 
Vertica database tables, including:
 * generating `create` statements for each of the tables.
 * transforming `ros::Time` to `SQL timestamp` types. 
 * uniquely identify records from multiple trips

##Current handling of nested types:
* rosbag2parquet flattens nested structs (eg Headers)
* rosbag2parquet currently skips fields of type variable sized arrays 
of complex types. (eg laserscan, velodyne packet and TF)
* rosbag2parquet skips fields of type fixed sized arrays of non-primitive types.
(eg some types are using int32[] for opaque data, 
not uint8[], and so that blob is being skipped)

Handling all of those is well within our reach, but
such data is not being used by our current queries. 
Feel free to request it if you need it,
preferably with a python program we can run to make sure
the result from the `.bag` looks just like the result
from the `.parquet` data.

## Testing: 
Currently we test this on some bagfiles we have, and run
queries on those files that we know have certain results.
We are getting to the point where a few sanity checks 
would save time.
 
## Performance:
Given enough IO bandwidth, the current implementation
bottlenecks on a single CPU consuming a non-compressed
30GB bagfile at about 80MB/s (and writing ~ 1/5  of that 
output size in SNAPPY format, depending on the types, but recall
we are skipping laser scan data). We haven't done
any tuning on the parquet side in terms of customizing
encoding.

## Dependencies:
See CMakefile. We have one on ros_instrospection 
type descriptions which we should make go away.