#!/bin/bash

#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

if [ -z "$(command -v protoc)" ]; then
  echo "The protoc tool is required before you can run Hydro locally."
  echo "Please install protoc manually, or use the scripts in" \
    "hydro-project/common to install dependencies before proceeding."
  exit 1
fi

cd $HYDRO_HOME/cloudburst
git pull origin master

cd $HYDRO_HOME/cluster

# Set up the shared Python package to put compile Protobuf definitions in.
rm -rf $HYDRO_HOME/cluster/hydro/shared/proto
mkdir $HYDRO_HOME/cluster/hydro/shared/proto
touch $HYDRO_HOME/cluster/hydro/shared/proto/__init__.py

# Compile shared Protobufs.
protoc -I=common/proto --python_out=$HYDRO_HOME/cluster/hydro/shared/proto shared.proto
protoc -I=common/proto --python_out=$HYDRO_HOME/cluster/hydro/shared/proto cloudburst.proto

# Compile the Protobufs to receive Anna metadata.
cd $HYDRO_HOME/anna
protoc -I=include/proto --python_out=$HYDRO_HOME/cluster/hydro/shared/proto metadata.proto

# Compile the Protobufs to receive Cloudburst metadata.
cd $HYDRO_HOME/cloudburst
protoc -I=proto --python_out=$HYDRO_HOME/cluster/hydro/shared/proto internal.proto

cd $HYDRO_HOME/cluster

# NOTE: This is a hack. We have to do this because the protobufs are not
# packaged properly (in the protobuf definitions). This isn't an issue for C++
# builds, because all the header files are in one place, but it breaks our
# Python imports. Consider how to fix this in the future.
if [[ "$OSTYPE" = "darwin"* ]]; then
  sed -i '' "s/import shared_pb2/from . import shared_pb2/g" $(find hydro/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
  sed -i '' "s/import cloudburst_pb2/from . import cloudburst_pb2/g" $(find hydro/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
else
  # We assume other linux distributions
  sed -i "s|import shared_pb2|from . import shared_pb2|g" $(find hydro/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
  sed -i "s|import cloudburst_pb2|from . import cloudburst_pb2|g" $(find hydro/shared/proto | grep pb2 | grep -v pyc | grep -v internal)
fi
