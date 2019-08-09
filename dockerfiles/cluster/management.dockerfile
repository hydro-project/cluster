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

FROM hydroproject/base:latest
MAINTAINER Vikram Sreekanti <vsreekanti@gmail.com> version: 0.1

ARG repo_org=hydro-project
ARG source_branch=master
ARG build_branch=docker-build

USER root

# Install kops. Downloads a precompiled executable and copies it into place.
RUN wget -O kops https://github.com/kubernetes/kops/releases/download/$(curl -s https://api.github.com/repos/kubernetes/kops/releases/latest | grep -Po '"tag_name": "\K.*?(?=")')/kops-linux-amd64
RUN chmod +x ./kops
RUN mv ./kops /usr/local/bin/

# Install kubectl. Downloads a precompiled executable and copies it into place.
RUN wget -O kubectl https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl

# Create the directory where the kubecfg is stored. The startup scripts will
# copy the actual kubecfg for the cluster into this directory at startup time.
RUN mkdir $HOME/.kube

# NOTE: It doesn't make sense to try to set up the kops user here because the
# person running this may want to configure that specifically. The getting
# started docs link to those instructions explicitly, so we can assume that
# it's done before we get to this point.
WORKDIR $HYDRO_HOME/cluster
RUN git remote remove origin 
RUN git remote add origin https://github.com/$repo_org/cluster
RUN git fetch origin && git checkout -b $build_branch origin/$source_branch
WORKDIR /

COPY start-management.sh /
CMD bash start-management.sh
