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
import os
import random
import time
import sys

import zmq

from anna.zmq_util import SocketCache

from hydro.management.scaler.default_scaler import DefaultScaler
from hydro.management.policy.default_policy import DefaultHydroPolicy
from hydro.management.util import (
    get_monitoring_depart_address,
    get_routing_depart_address,
    get_routing_seed_address,
    get_storage_depart_address,
    send_message
)
from hydro.shared import util
from hydro.shared.proto.internal_pb2 import ThreadStatus, ExecutorStatistics
from hydro.shared.proto.shared_pb2 import StringSet
from hydro.shared.proto.metadata_pb2 import ClusterMembership, MEMORY

REPORT_PERIOD = 5

PIN_ACCEPT_PORT = '5010'

logging.basicConfig(filename='log_management.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')


def run(self_ip):
    context = zmq.Context(1)

    pusher_cache = SocketCache(context, zmq.PUSH)

    restart_pull_socket = context.socket(zmq.REP)
    restart_pull_socket.bind('tcp://*:7000')

    churn_pull_socket = context.socket(zmq.PULL)
    churn_pull_socket.bind('tcp://*:7001')

    list_executors_socket = context.socket(zmq.PULL)
    list_executors_socket.bind('tcp://*:7002')

    function_status_socket = context.socket(zmq.PULL)
    function_status_socket.bind('tcp://*:7003')

    list_schedulers_socket = context.socket(zmq.REP)
    list_schedulers_socket.bind('tcp://*:7004')

    executor_depart_socket = context.socket(zmq.PULL)
    executor_depart_socket.bind('tcp://*:7005')

    statistics_socket = context.socket(zmq.PULL)
    statistics_socket.bind('tcp://*:7006')

    pin_accept_socket = context.socket(zmq.PULL)
    pin_accept_socket.setsockopt(zmq.RCVTIMEO, 10000) # 10 seconds.
    pin_accept_socket.bind('tcp://*:' + PIN_ACCEPT_PORT)

    poller = zmq.Poller()
    poller.register(restart_pull_socket, zmq.POLLIN)
    poller.register(churn_pull_socket, zmq.POLLIN)
    poller.register(function_status_socket, zmq.POLLIN)
    poller.register(list_executors_socket, zmq.POLLIN)
    poller.register(list_schedulers_socket, zmq.POLLIN)
    poller.register(executor_depart_socket, zmq.POLLIN)
    poller.register(statistics_socket, zmq.POLLIN)

    add_push_socket = context.socket(zmq.PUSH)
    add_push_socket.connect('ipc:///tmp/node_add')

    remove_push_socket = context.socket(zmq.PUSH)
    remove_push_socket.connect('ipc:///tmp/node_remove')

    client, _ = util.init_k8s()

    scaler = DefaultScaler(self_ip, context, add_push_socket, remove_push_socket, pin_accept_socket)
    policy = DefaultHydroPolicy(scaler)

    # Tracks the self-reported statuses of each executor thread in the system.
    executor_statuses = {}

    # Tracks of which executors are departing. This is used to ensure all
    # threads acknowledge that they are finished before we remove a thread from
    # the system.
    departing_executors = {}

    # Tracks how often each function is called.
    function_frequencies = {}

    # Tracks the aggregated runtime for each function.
    function_runtimes = {}

    # Tracks the arrival times of DAG requests.
    arrival_times = {}

    # Tracks how often each DAG is called.
    dag_frequencies = {}

    # Tracks how long each DAG request spends in the system, end to end.
    dag_runtimes = {}

    start = time.time()
    while True:
        socks = dict(poller.poll(timeout=1000))

        if (churn_pull_socket in socks and socks[churn_pull_socket] ==
                zmq.POLLIN):
            msg = churn_pull_socket.recv_string()
            args = msg.split(':')

            if args[0] == 'add':
                scaler.add_vms(args[2], args[1])
            elif args[0] == 'remove':
                scaler.remove_vms(args[2], args[1])

        if (restart_pull_socket in socks and socks[restart_pull_socket] ==
                zmq.POLLIN):
            msg = restart_pull_socket.recv_string()
            args = msg.split(':')

            pod = util.get_pod_from_ip(client, args[1])
            count = str(pod.status.container_statuses[0].restart_count)

            restart_pull_socket.send_string(count)

        if (list_executors_socket in socks and socks[list_executors_socket] ==
                zmq.POLLIN):
            # We can safely ignore this message's contents, and the response
            # does not depend on it.
            response_ip = list_executors_socket.recv_string()

            ips = StringSet()
            for ip in util.get_pod_ips(client, 'role=function'):
                ips.keys.append(ip)
            for ip in util.get_pod_ips(client, 'role=gpu'):
                ips.keys.append(ip)

            sckt = pusher_cache.get(response_ip)
            sckt.send(ips.SerializeToString())

        if (function_status_socket in socks and
                socks[function_status_socket] == zmq.POLLIN):
            # Dequeue all available ThreadStatus messages rather than doing
            # them one at a time---this prevents starvation if other operations
            # (e.g., pin) take a long time.
            while True:
                status = ThreadStatus()
                try:
                    status.ParseFromString(function_status_socket.recv(zmq.DONTWAIT))
                except:
                    break # We've run out of messages.

                key = (status.ip, status.tid)

                # If this executor is one of the ones that's currently departing,
                # we can just ignore its status updates since we don't want
                # utilization to be skewed downwards. The reason we might still
                # receive this message is because the depart message may not have
                # arrived when this was sent.
                if key[0] in departing_executors:
                    continue

                executor_statuses[key] = status
                logging.info(('Received thread status update from %s:%d: %.4f ' +
                              'occupancy, %d functions pinned') %
                             (status.ip, status.tid, status.utilization,
                              len(status.functions)))

        if (list_schedulers_socket in socks and
                socks[list_schedulers_socket] == zmq.POLLIN):
            # We can safely ignore this message's contents, and the response
            # does not depend on it.
            list_schedulers_socket.recv_string()

            ips = StringSet()
            for ip in util.get_pod_ips(client, 'role=scheduler'):
                ips.keys.append(ip)

            list_schedulers_socket.send(ips.SerializeToString())

        if (executor_depart_socket in socks and
                socks[executor_depart_socket] == zmq.POLLIN):
            ip = executor_depart_socket.recv_string()
            departing_executors[ip] -= 1

            # We wait until all the threads at this executor have acknowledged
            # that they are ready to leave, and we then remove the VM from the
            # system.
            if departing_executors[ip] == 0:
                logging.info('Removing node with ip %s' % ip)
                scaler.remove_vms('function', ip)
                del departing_executors[ip]

        if (statistics_socket in socks and
                socks[statistics_socket] == zmq.POLLIN):
            stats = ExecutorStatistics()
            stats.ParseFromString(statistics_socket.recv())

            # Aggregates statistics reported for individual functions including
            # call frequencies, processed requests, and total runtimes.
            for fstats in stats.functions:
                fname = fstats.name

                if fname not in function_frequencies:
                    function_frequencies[fname] = 0

                if fname not in function_runtimes:
                    function_runtimes[fname] = (0.0, 0)

                if fstats.runtime:
                    old_latency = function_runtimes[fname]

                    # This tracks how many calls were processed for the
                    # function and the length of the total runtime of all
                    # calls.
                    function_runtimes[fname] = (
                          old_latency[0] + sum(fstats.runtime),
                          old_latency[1] + fstats.call_count)
                else:
                    # This tracks how many calls are made to the function.
                    function_frequencies[fname] += fstats.call_count

            # Aggregates statistics for DAG requests, including call
            # frequencies, arrival rates, and end-to-end runtimes.
            for dstats in stats.dags:
                dname = dstats.name

                # Tracks the interarrival rates of requests to this function as
                # perceived by the scheduler.
                if dname not in arrival_times:
                    arrival_times[dname] = []

                arrival_times[dname] += list(dstats.interarrival)

                # Tracks how many calls to this DAG were received.
                if dname not in dag_frequencies:
                    dag_frequencies[dname] = 0

                dag_frequencies[dname] += dstats.call_count

                # Tracks the end-to-end runtime of individual requests
                # completed in the last epoch.
                if dname not in dag_runtimes:
                    dag_runtimes[dname] = []

                for rt in dstats.runtimes:
                    dag_runtimes[dname].append(rt)

        end = time.time()
        if end - start > REPORT_PERIOD:
            logging.info('Checking hash ring...')
            check_hash_ring(client, context)

            # Invoke the configured policy to check system load and respond
            # appropriately.
            policy.replica_policy(function_frequencies, function_runtimes,
                                  dag_runtimes, executor_statuses,
                                  arrival_times)
            policy.executor_policy(executor_statuses, departing_executors)

            # Clears all metadata that was passed in for this epoch.
            function_runtimes.clear()
            function_frequencies.clear()
            dag_runtimes.clear()
            arrival_times.clear()

            # Restart the timer for the next reporting epoch.
            start = time.time()


def check_hash_ring(client, context):
    route_ips = util.get_pod_ips(client, 'role=routing')

    # If there are no routing nodes in the system currently, the system is
    # still starting, so we do nothing.
    if not route_ips:
        return

    ip = random.choice(route_ips)

    # Retrieve a list of all current members of the cluster.
    socket = context.socket(zmq.REQ)
    socket.connect(get_routing_seed_address(ip, 0))
    socket.send_string('')
    resp = socket.recv()

    cluster = ClusterMembership()
    cluster.ParseFromString(resp)
    tiers = cluster.tiers

    # If there are no tiers, then we don't need to evaluate anything.
    if len(tiers) == 0:
        return
    elif len(tiers) == 1:
        # If there is one tier, it will be the memory tier.
        mem_tier, ebs_tier = tiers[0], None
    else:
        # If there are two tiers, we need to make sure that we assign the
        # correct tiers as the memory and EBS tiers, respectively.
        if tiers[0].tier_id == MEMORY:
            mem_tier = tiers[0]
            ebs_tier = tiers[1]
        else:
            mem_tier = tiers[1]
            ebs_tier = tiers[0]

    # Queries the Kubernetes master for the list of memory nodes its aware of
    # -- if any of the nodes in the hash ring aren't currently running, we add
    # those the departed list.
    mem_ips = util.get_pod_ips(client, 'role=memory')
    departed = []
    for node in mem_tier.servers:
        if node.private_ip not in mem_ips:
            departed.append(('0', node))

    # Performs the same process for the EBS tier if it exists.
    ebs_ips = []
    if ebs_tier:
        ebs_ips = util.get_pod_ips(client, 'role=ebs')
        for node in ebs_tier.servers:
            if node.private_ip not in ebs_ips:
                departed.append(('1', node))

    logging.info('Found %d departed nodes.' % (len(departed)))
    mon_ips = util.get_pod_ips(client, 'role=monitoring')
    storage_ips = mem_ips + ebs_ips

    # For each departed node the cluster is unaware of, we inform all storage
    # nodes, all monitoring nodes, and all routing nodes that it has departed.
    for pair in departed:
        logging.info('Informing cluster that node %s/%s has departed.' %
                     (pair[1].public_ip, pair[1].private_ip))

        msg = pair[0] + ':' + pair[1].public_ip + ':' + pair[1].private_ip

        # NOTE: In this code, we are presuming there are 4 threads per
        # storage/routing node. If there are more, this will be buggy; if there
        # are fewer, this is fine as the messages will go into the void.
        for ip in storage_ips:
            for t in range(4):
                send_message(context, msg, get_storage_depart_address(ip,
                                                                      t))

        msg = 'depart:' + msg
        for ip in route_ips:
            for t in range(4):
                send_message(context, msg, get_routing_depart_address(ip,
                                                                      t))

        for ip in mon_ips:
            send_message(context, msg, get_monitoring_depart_address(ip))


if __name__ == '__main__':
    # We wait for this file to appear before starting the management server,
    # so we don't make policy decisions before the cluster has finished
    # spinning up.
    while not os.path.isfile('/hydro/setup_complete'):
        pass

    # Waits until the kubecfg file is copied into the pod because we cannot
    # perform any Kubernetes operations without it.
    while not os.path.isfile(os.path.join(os.environ['HOME'],
                                          '.kube/config')):
        pass

    run(sys.argv[1])
