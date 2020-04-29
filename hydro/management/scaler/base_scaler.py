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


class BaseScaler():
    def __init__(self):
        raise NotImplementedError

    def replicate_function(self, fname, num_replicas, function_locations,
                           executors):
        '''
        Adds num_replicas more copies of the function named fname. The
        function_locations map is used to exclude the executor threads that
        already have functions pinned on them.
        '''
        raise NotImplementedError

    def dereplicate_function(self, fname, num_replicas, function_locations):
        '''
        Removes num_replicas of the function named fname from the list of
        locations that are stored in function_locations.
        '''
        raise NotImplementedError

    def add_vms(self, kind, count):
        '''
        Add a number (count) of VMs of a certain kind (currently support:
        memory, ebs, function).
        '''
        raise NotImplementedError

    def remove_vms(self, kind, ip):
        '''
        Removes a particular node (denoted by the the IP address ip) from the
        system; the support kinds are memory, ebs, and function.
        '''
        raise NotImplementedError
