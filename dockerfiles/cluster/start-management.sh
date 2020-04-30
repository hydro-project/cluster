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

IP=`ifconfig eth0 | grep 'inet addr:' | grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1 }'`

# Copies AWS environment variables for accessing the user's AWS account into
# the ~/.aws/config file.
mkdir -p ~/.aws
echo "[default]\nregion = us-east-1" > ~/.aws/config
echo "[default]\naws_access_key_id = $AWS_ACCESS_KEY_ID\naws_secret_access_key = $AWS_SECRET_ACCESS_KEY" > ~/.aws/credentials
mkdir -p ~/.ssh

cd $HYDRO_HOME/anna
git remote remove origin
git remote add origin https://github.com/$ANNA_REPO_ORG/anna
while ! (git fetch -p origin)
do
  echo "git fetch failed, retrying"
done
git checkout -b brnch origin/$ANNA_REPO_BRANCH
git submodule sync
git submodule update

cd client/python && python3.6 setup.py install --prefix=$HOME/.local

cd $HYDRO_HOME/cluster

# Move to the desired branch on the desired fork. If none is specified, we
# default to the master branch on hydro-project/cluster.
if [[ -z "$REPO_ORG" ]]; then
  REPO_ORG="hydro-project"
fi

if [[ -z "$REPO_BRANCH" ]]; then
  REPO_BRANCH="master"
fi

git remote remove origin
git remote add origin https://github.com/$REPO_ORG/cluster
while ! (git fetch -p origin)
do
  echo "git fetch failed, retrying"
done
git checkout -b brnch origin/$REPO_BRANCH
git submodule sync
git submodule update

# Generate compiled Python protobuf libraries from other Hydro project
# repositories. This is really a hack, but it shouldn't matter too much because
# this code should only ever be run in cluster mode, where this will be
# isolated from the user.
./scripts/compile-proto.sh

# Start the management servers. Add the current directory to the PYTHONPATH to
# be able to run its scripts.
cd $HYDRO_HOME/cluster
export PYTHONPATH=$PYTHONPATH:$(pwd)
python3.6 hydro/management/k8s_server.py &
python3.6 hydro/management/management_server.py $IP
