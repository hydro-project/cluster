#!/usr/bin/env python3

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

import argparse
import os

import boto3

from hydro.cluster.add_nodes import batch_add_nodes
from hydro.shared import util

BATCH_SIZE = 100

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))

def create_cluster(mem_count, ebs_count, func_count, gpu_count, sched_count,
                   route_count, bench_count, cfile, ssh_key, cluster_name,
                   kops_bucket, aws_key_id, aws_key):

    if 'HYDRO_HOME' not in os.environ:
        raise ValueError('HYDRO_HOME environment variable must be set to be '
                         + 'the directory where all Hydro project repos are '
                         + 'located.')
    prefix = os.path.join(os.environ['HYDRO_HOME'], 'cluster/hydro/cluster')

    util.run_process(['./create_cluster_object.sh', kops_bucket, ssh_key])

    client, apps_client = util.init_k8s()

    print('Creating management pods...')
    management_spec = util.load_yaml('yaml/pods/management-pod.yml', prefix)
    env = management_spec['spec']['containers'][0]['env']

    util.replace_yaml_val(env, 'AWS_ACCESS_KEY_ID', aws_key_id)
    util.replace_yaml_val(env, 'AWS_SECRET_ACCESS_KEY', aws_key)
    util.replace_yaml_val(env, 'KOPS_STATE_STORE', kops_bucket)
    util.replace_yaml_val(env, 'HYDRO_CLUSTER_NAME', cluster_name)

    client.create_namespaced_pod(namespace=util.NAMESPACE, body=management_spec)

    # Waits until the management pod starts to move forward -- we need to do
    # this because other pods depend on knowing the management pod's IP address.
    management_ip = util.get_pod_ips(client, 'role=management', is_running=True)[0]

    # Create the NVidia kubernetes plugin DaemonSet that enables GPU accesses.
    nvidia_ds_exists = True
    try:
        apps_client.read_namespaced_daemon_set('nvidia-device-plugin-daemonset', namespace='kube-system')
    except: # Throws an error if the DS doesnt' exist.
        nvidia_ds_exists = False

    if not nvidia_ds_exists:
        os.system('wget https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/1.0.0-beta5/nvidia-device-plugin.yml > /dev/null 2>&1')

        ds_spec = util.load_yaml('nvidia-device-plugin.yml')
        apps_client.create_namespaced_daemon_set(namespace='kube-system', body=ds_spec)

        os.system('rm nvidia-device-plugin.yml')

    # Copy kube config file to management pod, so it can execute kubectl
    # commands, in addition to SSH keys and KVS config.
    management_podname = management_spec['metadata']['name']
    kcname = management_spec['spec']['containers'][0]['name']

    os.system('cp %s anna-config.yml' % cfile)
    kubecfg = os.path.join(os.environ['HOME'], '.kube/config')
    util.copy_file_to_pod(client, kubecfg, management_podname, '/root/.kube/',
                          kcname)
    util.copy_file_to_pod(client, ssh_key, management_podname, '/root/.ssh/',
                          kcname)
    util.copy_file_to_pod(client, ssh_key + '.pub', management_podname,
                          '/root/.ssh/', kcname)
    util.copy_file_to_pod(client, 'anna-config.yml', management_podname,
                          '/hydro/anna/conf/', kcname)

    # Start the monitoring pod.
    mon_spec = util.load_yaml('yaml/pods/monitoring-pod.yml', prefix)
    util.replace_yaml_val(mon_spec['spec']['containers'][0]['env'], 'MGMT_IP',
                          management_ip)
    client.create_namespaced_pod(namespace=util.NAMESPACE, body=mon_spec)

    # Wait until the monitoring pod is finished creating to get its IP address
    # and then copy KVS config into the monitoring pod.
    util.get_pod_ips(client, 'role=monitoring')
    util.copy_file_to_pod(client, 'anna-config.yml',
                          mon_spec['metadata']['name'],
                          '/hydro/anna/conf/',
                          mon_spec['spec']['containers'][0]['name'])
    os.system('rm anna-config.yml')

    print('Creating %d routing nodes...' % (route_count))
    batch_add_nodes(client, apps_client, cfile, ['routing'], [route_count], BATCH_SIZE, prefix)
    util.get_pod_ips(client, 'role=routing')

    print('Creating %d memory, %d ebs node(s)...' %
          (mem_count, ebs_count))
    batch_add_nodes(client, apps_client, cfile, ['memory', 'ebs'], [mem_count,
                                                                    ebs_count],
                    BATCH_SIZE, prefix)

    print('Creating routing service...')
    service_spec = util.load_yaml('yaml/services/routing.yml', prefix)
    # Only create the routing service if it isn't up already
    # (e.g. from a previous execution of the script).
    if util.get_service_address(client, 'routing-service') is None:
        client.create_namespaced_service(namespace=util.NAMESPACE,
                                         body=service_spec)

    print('Adding %d scheduler nodes...' % (sched_count))
    batch_add_nodes(client, apps_client, cfile, ['scheduler'], [sched_count],
                    BATCH_SIZE, prefix)
    util.get_pod_ips(client, 'role=scheduler')

    print('Adding %d function, %d GPU nodes...' % (func_count, gpu_count))
    batch_add_nodes(client, apps_client, cfile, ['function', 'gpu'],
                    [func_count, gpu_count], BATCH_SIZE, prefix)

    print('Creating function service...')
    service_spec = util.load_yaml('yaml/services/function.yml', prefix)
    if util.get_service_address(client, 'function-service') is None:
        client.create_namespaced_service(namespace=util.NAMESPACE,
                                         body=service_spec)

    print('Adding %d benchmark nodes...' % (bench_count))
    batch_add_nodes(client, apps_client, cfile, ['benchmark'], [bench_count],
                    BATCH_SIZE, prefix)

    print('Finished creating all pods...')
    os.system('touch setup_complete')
    util.copy_file_to_pod(client, 'setup_complete', management_podname, '/hydro',
                          kcname)
    os.system('rm setup_complete')

    sg_name = 'nodes.' + cluster_name
    sg = ec2_client.describe_security_groups(
          Filters=[{'Name': 'group-name',
                    'Values': [sg_name]}])['SecurityGroups'][0]

    print('Authorizing ports for routing service...')

    permission = [{
        'FromPort': 6200,
        'IpProtocol': 'tcp',
        'ToPort': 6203,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    }]

    ec2_client.authorize_security_group_ingress(GroupId=sg['GroupId'],
                                                IpPermissions=permission)

    routing_svc_addr = util.get_service_address(client, 'routing-service')
    function_svc_addr = util.get_service_address(client, 'function-service')
    print('The routing service can be accessed here: \n\t%s' %
          (routing_svc_addr))
    print('The function service can be accessed here: \n\t%s' %
          (function_svc_addr))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''Creates a Hydro cluster
                                     using Kubernetes and kops. If no SSH key
                                     is specified, we use the default SSH key
                                     (~/.ssh/id_rsa), and we expect that the
                                     correponding public key has the same path
                                     and ends in .pub.

                                     If no configuration file base is
                                     specified, we use the default
                                     ($HYDRO_HOME/anna/conf/anna-base.yml).''')

    parser.add_argument('-m', '--memory', nargs=1, type=int, metavar='M',
                        help='The number of memory nodes to start with ' +
                        '(required)', dest='memory', required=True)
    parser.add_argument('-r', '--routing', nargs=1, type=int, metavar='R',
                        help='The number of routing  nodes in the cluster ' +
                        '(required)', dest='routing', required=True)
    parser.add_argument('-f', '--function', nargs=1, type=int, metavar='F',
                        help='The number of function nodes to start with ' +
                        '(required)', dest='function', required=True)
    parser.add_argument('-s', '--scheduler', nargs=1, type=int, metavar='S',
                        help='The number of scheduler nodes to start with ' +
                        '(required)', dest='scheduler', required=True)
    parser.add_argument('-g', '--gpu', nargs='?', type=int, metavar='G',
                        help='The number of GPU nodes to start with ' +
                        '(optional)', dest='gpu', default=0)
    parser.add_argument('-e', '--ebs', nargs='?', type=int, metavar='E',
                        help='The number of EBS nodes to start with ' +
                        '(optional)', dest='ebs', default=0)
    parser.add_argument('-b', '--benchmark', nargs='?', type=int, metavar='B',
                        help='The number of benchmark nodes in the cluster ' +
                        '(optional)', dest='benchmark', default=0)
    parser.add_argument('--conf', nargs='?', type=str,
                        help='The configuration file to start the cluster with'
                        + ' (optional)', dest='conf',
                        default=os.path.join(os.getenv('HYDRO_HOME', '..'),
                                             'anna/conf/anna-base.yml'))
    parser.add_argument('--ssh-key', nargs='?', type=str,
                        help='The SSH key used to configure and connect to ' +
                        'each node (optional)', dest='sshkey',
                        default=os.path.join(os.environ['HOME'],
                                             '.ssh/id_rsa'))

    cluster_name = util.check_or_get_env_arg('HYDRO_CLUSTER_NAME')
    kops_bucket = util.check_or_get_env_arg('KOPS_STATE_STORE')
    aws_key_id = util.check_or_get_env_arg('AWS_ACCESS_KEY_ID')
    aws_key = util.check_or_get_env_arg('AWS_SECRET_ACCESS_KEY')

    args = parser.parse_args()

    create_cluster(args.memory[0], args.ebs, args.function[0], args.gpu,
                   args.scheduler[0], args.routing[0], args.benchmark,
                   args.conf, args.sshkey, cluster_name, kops_bucket,
                   aws_key_id, aws_key)
