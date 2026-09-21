# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/mnt/c/Users/manas/kronos-db/build/_deps/httplib_src-src"
  "/mnt/c/Users/manas/kronos-db/build/_deps/httplib_src-build"
  "/mnt/c/Users/manas/kronos-db/build/_deps/httplib_src-subbuild/httplib_src-populate-prefix"
  "/mnt/c/Users/manas/kronos-db/build/_deps/httplib_src-subbuild/httplib_src-populate-prefix/tmp"
  "/mnt/c/Users/manas/kronos-db/build/_deps/httplib_src-subbuild/httplib_src-populate-prefix/src/httplib_src-populate-stamp"
  "/mnt/c/Users/manas/kronos-db/build/_deps/httplib_src-subbuild/httplib_src-populate-prefix/src"
  "/mnt/c/Users/manas/kronos-db/build/_deps/httplib_src-subbuild/httplib_src-populate-prefix/src/httplib_src-populate-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/mnt/c/Users/manas/kronos-db/build/_deps/httplib_src-subbuild/httplib_src-populate-prefix/src/httplib_src-populate-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/mnt/c/Users/manas/kronos-db/build/_deps/httplib_src-subbuild/httplib_src-populate-prefix/src/httplib_src-populate-stamp${cfgdir}") # cfgdir has leading slash
endif()
