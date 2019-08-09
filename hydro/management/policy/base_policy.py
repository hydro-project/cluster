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


class BaseHydroPolicy():
    def __init__(self):
        raise NotImplementedError

    def replica_policy(self, function_frequencies, function_runtimes,
                       dag_runtimes, executor_statuses, arrival_times):
        '''
        This policy determines how many replicas of a particular function
        should be deployed in the system. This assumes that there are enough
        resources to deploy those functions, and if there are not, that should
        be taken care of by the executor policy when it is invoked.

        The metrics that this policy is evaluated on include call frequencies,
        function runtimes, dag runtimes, and request arrival rates.
        '''

        raise NotImplementedError

    def executor_policy(self, executor_statuses, departing_executors):
        '''
        This policy determines how many executors should be added to or removed
        from the systtem. This is based on how loaded the executors are, how
        much slack there is in the system in terms of both compute cycles and
        free cores to pin functions onto.
        '''
        raise NotImplementedError
