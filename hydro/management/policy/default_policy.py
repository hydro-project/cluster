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

import logging
import math
import random
import time

from hydro.management.policy.base_policy import BaseHydroPolicy
from hydro.management.util import (
    get_executor_depart_address,
    NUM_EXEC_THREADS,
    send_message
)
from hydro.shared.proto.internal_pb2 import CPU, GPU

EXECUTOR_REPORT_PERIOD = 5


class DefaultHydroPolicy(BaseHydroPolicy):
    def __init__(self, scaler, max_utilization=.60, min_utilization=.10,
                 max_pin_count=.8, max_latency_deviation=1.25,
                 scale_increase=4, grace_period=120):
        self.grace_start = 0

        self.scaler = scaler

        self.max_utilization = max_utilization
        self.min_utilization = min_utilization
        self.max_pin_count = max_pin_count
        self.max_latency_deviation = max_latency_deviation
        self.scale_increase = scale_increase
        self.grace_period = grace_period

        self.latency_history = {}
        self.function_locations = {}

    def replica_policy(self, function_frequencies, function_runtimes,
                       dag_runtimes, executor_statuses, arrival_times):
        # Construct a reverse index that tracks where each function is
        # currently replicated.
        self.function_locations = {}
        for key in executor_statuses:
            status = executor_statuses[key]
            for fname in status.functions:
                if fname not in self.function_locations:
                    self.function_locations[fname] = set()

                self.function_locations[fname].add(key)

        cpu_executors = set()
        gpu_executors = set()
        for key in executor_statuses:
            status = executor_statuses[key]
            if status.type == CPU:
                cpu_executors.add(key)
            else:
                gpu_executors.add(key)

        # Evaluate the policy decisions for each function that is reporting
        # metadata.
        for fname in function_frequencies:
            runtime = function_runtimes[fname]
            call_count = function_frequencies[fname]

            if call_count == 0 or runtime[0] == 0:
                continue

            avg_latency = runtime[0] / runtime[1]
            num_replicas = len(self.function_locations[fname])
            thruput = float(num_replicas *
                            EXECUTOR_REPORT_PERIOD) * (1 / avg_latency)
            if fname in self.latency_history:
                historical, count = self.latency_history[fname]
            else:
                historical, count = 0.0, 0

            logging.info(('Function %s: %d calls, %.4f average latency, %.2f' +
                          ' thruput, %d replicas.') % (fname, call_count,
                                                       avg_latency, thruput,
                                                       num_replicas))

            if call_count > thruput * .7:
                # First, we compare the throughput of the system for a function
                # to the number of calls for it. We add replicas if the number
                # of calls exceeds a percentage of the throughput.
                increase = (math.ceil(call_count / (thruput * .7))
                            * num_replicas) - num_replicas + 1
                logging.info(('Function %s: %d calls in recent period exceeds'
                              + ' threshold. Adding %d replicas.') %
                             (fname, call_count, increase))
                self.scaler.replicate_function(fname, increase,
                                               self.function_locations,
                                               cpu_executors, gpu_executors)
            elif call_count < thruput * .1:
                pass
                # Similarly, we check to see if the call count is significantly
                # below the achieved throughput -- we then remove replicas.

                # cgwu: sometimes the call count is misleading because we
                # haven't gathered the count across all executors
                decrease = math.ceil((call_count / thruput) * num_replicas) + 1
                logging.info(('Function %s: %d calls in recent period under ' +
                              'threshold. Reducing to %d replicas.') %
                             (fname, call_count, decrease))
                self.scaler.dereplicate_function(fname, decrease,
                                                 self.function_locations)
            elif fname in self.latency_history:
                # Next, we look at historical perceived latency of requests
                # -- if the request is spending more time in the system than it
                # has in the past, we up the number of replicas.
                ratio = avg_latency / historical

                if ratio > self.max_latency_deviation:
                    ratio *= len(self.function_locations[fname])
                    num_replicas = (math.ceil(ratio) -
                                    len(self.function_locations[fname]) + 1)
                    logging.info(('Function %s: recent latency average (%.4f) '
                                  + 'is %.2f times the historical average. '
                                  + 'Adding %d replicas.')
                                 % (fname, avg_latency, ratio, num_replicas))

                    self.scaler.replicate_function(fname, num_replicas,
                                                   self.function_locations,
                                                   cpu_executors, gpu_executors)

            # Recalculates total runtime for this function and the historical
            # call count and updates latency history metadata.
            rt = runtime[0] + historical * count
            hist_count = runtime[1] + count
            avg_latency = rt / hist_count
            self.latency_history[fname] = (avg_latency, hist_count)

    def executor_policy(self, executor_statuses, departing_executors):
        # If no executors have joined yet, we don't need to calcuate anything.
        if len(executor_statuses) == 0:
            return

        # We institute a grace period (2 minutes by default) during which no
        # elasticity decisions are made. We start the grace period when we
        # decide to add or remove a VM and wait until after its over to make
        # sure we don't put the system in hysteresis.
        if time.time() < (self.grace_start + self.grace_period):
            return

        utilization_sum = 0.0
        pinned_function_count = 0
        for status in executor_statuses.values():
            utilization_sum += status.utilization
            pinned_function_count += len(status.functions)

        avg_utilization = utilization_sum / len(executor_statuses)
        avg_pinned_count = pinned_function_count / len(executor_statuses)
        num_nodes = len(executor_statuses) / NUM_EXEC_THREADS

        logging.info(('There are currently %d executor nodes active in the ' +
                     'system (%d threads).') % (num_nodes,
                                                len(executor_statuses)))
        logging.info('Average executor utilization: %.4f' % (avg_utilization))
        logging.info('Average pinned function count: %.2f' %
                     (avg_pinned_count))

        # We check to see if the average utilization or number of pinned
        # functions exceeds the policy's thresholds and add machines to the
        # system in both cases.
        if (avg_utilization > self.max_utilization or avg_pinned_count >
                self.max_pin_count):
            logging.info(('Average utilization is %.4f. Adding %d nodes to'
                          + ' cluster.') % (avg_utilization,
                                            self.scale_increase))

            self.scaler.add_vms('function', self.scale_increase)

            # start the grace period after adding nodes
            self.grace_start = time.time()

        # We also look at any individual nodes that might be overloaded. Since
        # we currently only pin one function per node, that means that function
        # is very expensive, so we proactively replicate it onto two other
        # threads.
        for status in executor_statuses.values():
            if status.utilization > .9:
                logging.info(('Node %s:%d has over 90%% utilization.'
                              + ' Replicating its functions.') % (status.ip,
                                                                  status.tid))

                executors = set(executor_statuses.keys())
                for fname in status.functions:
                    self.scaler.replicate_function(fname, 2,
                                                   self.function_locations,
                                                   executors)

        # We only decide to kill nodes if they are underutilized and if there
        # are at least 5 executors in the system -- we never scale down past
        # that.
        if avg_utilization < self.min_utilization and num_nodes > 5:
            ip = random.choice(list(executor_statuses.values())).ip
            logging.info(('Average utilization is %.4f, and there are %d '
                          + 'executors. Removing IP %s.') %
                         (avg_utilization, len(executor_statuses), ip))

            for tid in range(NUM_EXEC_THREADS):
                send_message(self.scaler.context, '',
                             get_executor_depart_address(ip, tid))

                if (ip, tid) in executor_statuses:
                    del executor_statuses[(ip, tid)]

            departing_executors[ip] = NUM_EXEC_THREADS

            # start the grace period after removing nodes
            self.grace_start = time.time()
