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

import zmq

NUM_EXEC_THREADS = 3

TCP_BASE = 'tcp://%s:%d'

EXECUTOR_DEPART_PORT = 4050
EXECUTOR_PIN_PORT = 4000
EXECUTOR_UNPIN_PORT = 4010

KVS_NODE_DEPART_PORT = 6050
ROUTING_SEED_PORT = 6350
ROUTING_NOTIFY_PORT = 6400
MONITORING_NOTIFY_PORT = 6600


def send_message(context, message, address):
    socket = context.socket(zmq.PUSH)
    socket.connect(address)
    
    if type(message) == str:
        socket.send_string(message)
    else:
        socket.send(message)


def get_executor_depart_address(ip, tid):
    return TCP_BASE % (ip, tid + EXECUTOR_DEPART_PORT)


def get_executor_pin_address(ip, tid):
    return TCP_BASE % (ip, tid + EXECUTOR_PIN_PORT)


def get_executor_unpin_address(ip, tid):
    return TCP_BASE % (ip, tid + EXECUTOR_UNPIN_PORT)


def get_routing_seed_address(ip, tid):
    return TCP_BASE % (ip, tid + ROUTING_SEED_PORT)


def get_storage_depart_address(ip, tid):
    return TCP_BASE % (ip, tid + KVS_NODE_DEPART_PORT)


def get_routing_depart_address(ip, tid):
    return TCP_BASE % (ip, tid + ROUTING_NOTIFY_PORT)


def get_monitoring_depart_address(ip):
    return TCP_BASE % (ip, MONITORING_NOTIFY_PORT)
