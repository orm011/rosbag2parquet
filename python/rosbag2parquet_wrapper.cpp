// Author(s): Sudeep Pillai (spillai@csail.mit.edu), Nick Rypkema (rypkema@mit.edu)
// License: MIT

#include <boost/python.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <boost/python/stl_iterator.hpp>
#include "rosbag2parquet.h"

namespace py = boost::python;

BOOST_PYTHON_FUNCTION_OVERLOADS(rosbag2parquet_overloads, 
                                rosbag2parquet, 2, 4)


BOOST_PYTHON_MODULE(rosbag2parquetpy)
{
  py::scope scope = py::scope();
  
  // py::def("rosbag2parquet", &rosbag2parquet_overloads,
  //         py::args("bagfile", "opath"), py::arg("max_mbs")=-1, py::arg("verbose")=false);
  py::def("rosbag2parquet", rosbag2parquet, rosbag2parquet_overloads());

} 
