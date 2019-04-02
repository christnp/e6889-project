# Install script for directory: /home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/python

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Release")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/python2.7/dist-packages/GooglePubSub" TYPE FILE FILES
    "/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/python/__init__.py"
    "/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/python/google_publisher_py_b.py"
    "/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/python/google_subscriber_py_b.py"
    )
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/python2.7/dist-packages/GooglePubSub" TYPE FILE FILES
    "/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/build/python/__init__.pyc"
    "/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/build/python/google_publisher_py_b.pyc"
    "/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/build/python/google_subscriber_py_b.pyc"
    "/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/build/python/__init__.pyo"
    "/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/build/python/google_publisher_py_b.pyo"
    "/home/christnp/Development/e6889/project/gnu-radio/gr-GooglePubSub/build/python/google_subscriber_py_b.pyo"
    )
endif()

