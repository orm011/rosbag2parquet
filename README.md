# rosbag2parquet
Rosbag2parquet transforms ROS .`bag` files into  query friendlier `.parquet` files

Why parquet?  

Parquet is a chunked-columnar format.  Chunked means a group of rows get placed together, columnar that the fields of those rows get stored in a columnar way. Each chunk has statistics on each column (min,max,count,count nulls) that allow one to decide whether a chunk has data of interest. Rosbag does something similar but only on a few select fields (timestamp, connection).  By making data columnar, it is possible to access a subset of the fields in a CompressedImage (eg, the header and frame_id) before deciding whether to incur I/O for the image blob itself. This is useful especially in bandwidth constrained data access (eg accessing csail NFS through 1Gbit ethernet).

Like in rosbag, whole chunks then get compressed. Parquet supports a lot more chunk compression formats in the compute-size tradeoff spectrum than rosbag. (Eg. SNAPPY, LZO, GZIP).  In principle, the fields in the same columns are more compressible placed together than as rows, though we dont' know if this is a big deal in our case.  Additionally Parquet supports per-column encoding schemes like Dictionary, run length and delta. These are there also to allow potentially faster querying.

Parquet has support for accessing small parts of large parquet files (or collections of files) without having to pay a large penalty in reading lots of the file or an initialization penalty to gather lots of metadata. Rosbag takes a long time to initialize itself, apparently reading metadata at multiple locations. (TODO, show plots). 

Multiple tools support parquet: eg. pyarrow loads parquet into arrow and pandas, and one can first pick which chunk of a large to load. Spark, Impala and Drill all allow one to query parquet files.   Some databases include foreign data wrapper for parquet files (eg, Vertica). In principle, postgres could have one implemented as well. This allows one to query with SQL without going through an extra load phase.   

Parquet allows taking multiple parquet files' metadata and merging it. This can help go from multiple single trip files to a virtual multi-trip file without requiring physically merging them.

Parquet also allows transparent flattening of datatypes. We are not using this component at the moment and it is unclear this is supported by most parquet reading tools, but with enough support on the read side, this may be a good way to handle the nesting inherent of rosmsg types. 

We are still learning whether these things are a big advantage in practice (or whether the tooling works). We may decide an alternative format is better.

Example:

```bash
$ ./rosbag2parquet.bin --bagfile ~/test_data/blue_100M.bag --outdir /tmp/
creating output directory /tmp/blue_100M_parquet_dir.36
******* Parquet schema: 
message Messages {
  required int64 seqno;
  required int32 time_sec (UINT_32);
  required int32 time_nsec (UINT_32);
  required int32 size (UINT_32);
  required int32 connection_id (UINT_32);
}
***********************
******* Parquet schema: 
message Connections {
  required int32 connection_id (UINT_32);
  required binary topic (UTF8);
  required binary datatype (UTF8);
  required binary md5sum (UTF8);
  required binary msg_def (UTF8);
  required binary callerid (UTF8);
}
***********************
******* found type rosgraph_msgs/Log
******* ROS msg definition: rosgraph_msgs/Log
  Header header
  byte level
  string name # name of the node
  string msg # message
  string file # file the message came from
  string function # function the message came from
  uint32 line # line the message came from
  string[] topics # topic names that the node publishes
******* Parquet schema: 
message rosgraph_msgs_Log {
  required int64 seqno;
  required int32 header_seq;
  required int32 header_stamp_sec;
  required int32 header_stamp_nsec;
  required binary header_frame_id (UTF8);
  required int32 level;
  required binary name (UTF8);
  required binary msg (UTF8);
  required binary file (UTF8);
  required binary function (UTF8);
  required int32 line;
}
...
...
Wrote 23719 records to 
/tmp/blue_100M_parquet_dir.36
$ ls -sh1 
total 102M
102M blue_100M.bag
$ cd  /tmp/blue_100M_parquet_dir.36/
$ ls -sh1 
total 41M
108K can_bus_ros_version_CanBusMsgStamped.parquet
 28K Connections.parquet
 36K conversions_EncoderStamped.parquet
4.0K diagnostic_msgs_DiagnosticArray.parquet
4.0K dynamic_reconfigure_ConfigDescription.parquet
4.0K dynamic_reconfigure_Config.parquet
 72K imu_3dm_gx4_FilterOutput.parquet
304K Messages.parquet
196K nav_msgs_Odometry.parquet
 12K rosgraph_msgs_Log.parquet
 12K sensor_msgs_CameraInfo.parquet
 40M sensor_msgs_CompressedImage.parquet
 20K sensor_msgs_FluidPressure.parquet
 72K sensor_msgs_Imu.parquet
 32K sensor_msgs_LaserScan.parquet
 36K sensor_msgs_MagneticField.parquet
8.0K sensor_msgs_NavSatFix.parquet
 80K serial_interface_SerialIntegerMsg.parquet
 36K std_msgs_Float64.parquet
8.0K tf2_msgs_TFMessage.parquet
188K theora_image_transport_Packet.parquet
4.0K velodyne_msgs_VelodyneScan.parquet
 28K vertica_load_tables.sql
```

*Current level of functionality:*
* It handles all primitive types other than duration (including strings).
* It handles byte arrays (eg, in Compressed_Image)
* It handles fixed sized arrays of any primitive type

## Output description:
A bagfile contains message data as well as bag metadata.
As shown above, we create two metadata tables (Messages and Connections)
to hold bag metadata.
 
* The messages table has an entry for every message in the 
  bag.
   
* The connections table holds most of the information stored
 in rosbag::ConnectionInfo objects. 
 
 We additionally create one parquet file per message type to store all messages
of that type.  Each message on each table gets a unique `seqno` that identifies
 it with its original position within the bagfile.

Additionally, we are also generating a `.sql` script with all
the code needed to load the output `.parquet` files into 
Vertica database tables, including:
 * generating `create` statements for each of the tables.
 * transforming `ros::Time` to `SQL timestamp` types. 
 * uniquely identify records from multiple trips loaded
 onto the same database

## Current handling of nested types:
* rosbag2parquet converts all rosmsg primitive types to parquet versions, including strings.
* rosbag2parquet flattens nested structs (eg Headers)
* rosbag2parquet skips array types of any kind (they get preserved as a blob, but are not flattened/queriable at the moment)

Handling all of those is well within our reach, but
such data is not being used by our current queries. 
Feel free to request it if you need it,
preferably with a python program we can run to make sure
the result from the `.bag` looks just like the result
from the `.parquet` data.

## Testing: 
We could use a small test to verify we handle different ros types well.

Currently we test this end to end on some bagfiles we have, and run
queries on those files that we know have certain results.

## Performance:
Given enough IO bandwidth, the current implementation
bottlenecks on a single CPU consuming a non-compressed
30GB bagfile at about 80MB/s (and writing ~ 1/5  of that 
output size in SNAPPY format, depending on the types, but recall
we are skipping laser scan data). We haven't done
any tuning on the parquet side in terms of customizing
encoding.

## Dependencies:
TODO: Would love to clean this up to make it easy to build.

### GoogleTest: 
cmake should download the repo and handle this without extra inolvement.

### Packages with Find*.cmake:
See CMakefile.  These will get found by cmake or reported 
at config time as failures. Ros kinetic is found this way.

### Packages without a Find*.cmake:
Lack of them will simply show up  as compile/link errors.
* Boost Filesystem (there must be a cmake for this, right?)
* Google flags 
* parquet-cpp 1.0 and apache arrow (you probably want to build this one 
  from srouce. Arrow gets downloaded and built for it)
o satisfy them you need to make sure they are in well known paths
or the right files show up in in CPLUS_INCLUDE_PATH, LIBRARY_PATH, LD_LIBRARY_PATH.

### Non-packaged software:
We have a dependency on ros_instrospection msg parsing and type descriptions.

To remove, we can write a tool to convert ros message definitions (text) into a C struct description. We should be able to go from that C struct to a parquet Schema object,  and we should be able to use that C struct to direct the parsing of a message.

Forked version available at: https://github.com/orm011/ros_type_introspection

To satisfy this one we need to download the git repo, build it and provide something like the following:

### Configuring build:
 ```cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=/home/orm/ros_type_introspection/build/devel/share/ros_type_introspection/cmake/ ..```
