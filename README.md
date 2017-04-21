# rosbag2parquet
Rosbag2parquet transforms ROS .`bag` files into 
query friendlier `.parquet` files

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

##Output description:
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
Implicit dependencies on:
* Boost Filesystem
* Google flags (does not come with cmake)
* parquet-cpp 1.0 (including indirect one on Arrow, neither
of which uses cmake)
* Our version of ros is kinetic.

See CMakefile for explicit ones.
We have a dependency on ros_instrospection msg parsing and type descriptions which we should make go away
also for both ease of installation and performance reasons.