// Author(s): TODO
// License: TODO

#include <boost/python.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <boost/python/stl_iterator.hpp>
#include "rosbag2parquet.h"

namespace py = boost::python;

BOOST_PYTHON_MODULE(rosbag2parquetpy)
{
  py::scope scope = py::scope();

  py::class_<info>("info")
    .def(py::init<>())
    .def_readonly("bagname", &info::bagname)
    .def_readonly("count", &info::count)
    .def_readonly("size", &info::size)
    ;
                   
  py::def("rosbag2parquet", rosbag2parquet, 
          (py::args("bagfile", "opath"),
           py::arg("max_mbs")=-1, py::arg("verbose")=false));

} 
