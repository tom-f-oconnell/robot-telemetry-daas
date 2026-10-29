# Locate the Apache Avro C++ library (libavrocpp). Ubuntu does not ship a reliable
# package for it, so the bridge Dockerfile builds avro-cpp 1.11.3 from source into
# /usr/local; this module finds either that or a distro/Conda install.
#
# Defines the imported target AvroCpp::avrocpp and sets AvroCpp_FOUND.

find_path(AvroCpp_INCLUDE_DIR
  NAMES avro/ValidSchema.hh
  HINTS ${AvroCpp_ROOT} ENV AvroCpp_ROOT /usr/local /usr /opt/avro
  PATH_SUFFIXES include
)
find_library(AvroCpp_LIBRARY
  NAMES avrocpp avrocpp_s
  HINTS ${AvroCpp_ROOT} ENV AvroCpp_ROOT /usr/local /usr /opt/avro
  PATH_SUFFIXES lib lib64
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(AvroCpp
  REQUIRED_VARS AvroCpp_LIBRARY AvroCpp_INCLUDE_DIR
)

if(AvroCpp_FOUND AND NOT TARGET AvroCpp::avrocpp)
  add_library(AvroCpp::avrocpp UNKNOWN IMPORTED)
  set_target_properties(AvroCpp::avrocpp PROPERTIES
    IMPORTED_LOCATION "${AvroCpp_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${AvroCpp_INCLUDE_DIR}"
  )
  # avro-cpp 1.11 still links against Boost (iostreams for the deflate codec, system, etc.).
  find_package(Boost QUIET COMPONENTS iostreams system filesystem)
  if(Boost_FOUND)
    set_property(TARGET AvroCpp::avrocpp APPEND PROPERTY
      INTERFACE_LINK_LIBRARIES Boost::iostreams Boost::system Boost::filesystem)
  endif()
endif()

mark_as_advanced(AvroCpp_INCLUDE_DIR AvroCpp_LIBRARY)
