#!/usr/bin/python
from __future__ import print_function
import sys
import rubrik_cdm
import getopt
import getpass
import urllib3
urllib3.disable_warnings()
import datetime
import pytz
import time
import threading
import random
from random import randrange
from pprint import pprint
import io
import os

try:
    import queue
except ImportError:
    import Queue as queue

def python_input(message):
    if int(sys.version[0]) > 2:
        val = input(message)
    else:
        val = raw_input(message)
    return(val)

def dprint(message):
    if DEBUG:
        if int(sys.version[0]) > 2:
            dfh = open(debug_log, 'a', encoding='utf-8')
            dfh.write(message + "\n")
        else:
            dfh = io.open(debug_log, 'a', encoding='utf-8')
            dfh.write(unicode(message) + "\n")
        dfh.close()
    return()
def oprint(message, fh):
    if not fh:
        print(message)
    else:
        fh.write(message + "\n")

def get_rubrik_nodes(rubrik, user, password, token):
    node_list = []
    cluster_network = rubrik.get('internal', '/cluster/me/network_interface', timeout=timeout)
    dprint("CLUSTER_NETWORK: ")
    dprint(str(cluster_network))
    for n in cluster_network['data']:
        if n['interfaceType'] == "Management":
            if token:
                try:
                    rbk_session = rubrik_cdm.Connect(n['ipAddresses'][0], api_token=token)
                except Exception as e:
                    sys.stderr.write("Error on " + n['ipAddresses'][0] + ": " + str(e) + ". Skipping\n")
                    continue
            else:
                try:
                    rbk_session = rubrik_cdm.Connect(n['ipAddresses'][0], user, password)
                except Exception as e:
                    sys.stderr.write("Error on " + n['ipAddresses'][0] + ": " + str(e) + ". Skipping\n")
                    continue
            try:
                node_list.append({'session': rbk_session, 'name': n['nodeName']})
            except KeyError:
                node_list.append({'session': rbk_session, 'name': n['node']})
    dprint("NODE_LIST: " + str(node_list))
    return(node_list)

def job_queue_length(thread_list):
    list_check = []
    for thread in threading.enumerate():
        if thread.name in thread_list:
            list_check.append(thread_name)
    dprint("JQD returns " + str(len(list_check)))
    return(len(list_check))

def usage():
    sys.stderr.write("Usage goes here\n")
    exit(0)

if __name__ == "__main__":
    backup = ""
    rubrik = ""
    user = ""
    password = ""
    fileset = ""
    date = ""
    latest = False
    share_id = ""
    restore_job = []
    physical = False
    snap_list = []
    restore_location = ""
    restore_share_id = ""
    restore_host_id = ""
    token = ""
    DEBUG = False
    VERBOSE = False
    REPORT_ONLY = False
    ofh = ""
    timeout = 360
    rubrik_cluster = []
    job_queue = queue.Queue()
    max_threads = 0
    thread_factor = 10
    debug_log = "debug_log.txt"
    large_trees = queue.Queue()
    parts = queue.Queue()
    SINGLE_NODE = False

    (optlist, args) = getopt.getopt(sys.argv[1:], 'b:f:c:hd:Dt:sm:M:vl', ['backup=', 'fileset=', 'creds=', 'date=',
                                                                          'help', 'DEBUG', 'token=', 'max_threads=',
                                                                          'thread-factor=', 'single_node', 'verbose',
                                                                          'latest'])
    for opt, a in optlist:
        if opt in ('-b', '--backup'):
            backup = a
        if opt in ('-f', '--fileset'):
            fileset = a
        if opt in ('-c', '--creds'):
            (user, password) = a.split(':')
        if opt in ('-h', '--help'):
            usage()
        if opt in ('-d', '--date'):
            date = a
            date_dt = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
            date_dt_s = datetime.datetime.strftime(date_dt, "Y-%m-%dT%H:%M:%S")
        if opt in ('-D', '--DEBUG'):
            VERBOSE = True
            DEBUG = True
            dfh = open(debug_log, "w")
            dfh.close()
        if opt in ('-s', '--single_node'):
            SINGLE_NODE = True
        if opt in ('-m', '--max_threads'):
            max_threads = int(a)
        if opt in ('-M', '--thread_factor'):
            thread_factor = int(a)
        if opt in ('-v', '--verbose'):
            VERBOSE = True
        if opt in ('-l', '--latest'):
            latest = True
    try:
        (rubrik_node, local_path) = args
    except:
        dprint("Usage Called.  No Rubrik Node/local path")
        usage()
    if not backup:
        backup = python_input("Backup: ")
    if not ':' in backup:
        physical = True
        host = backup
    else:
        physical = False
        (host, share) = backup.split(':')
        if share.startswith('/'):
            delim = '/'
        else:
            delim = "\\"
        initial_path = delim
    if not fileset:
        fileset = python_input("Fileset: ")
    if not token:
        if not user:
            user = python_input("User: ")
        if not password:
            password = getpass.getpass("Password: ")

    if token:
        rubrik = rubrik_cdm.Connect(rubrik_node, api_token=token)
    else:
        rubrik = rubrik_cdm.Connect(rubrik_node, user, password)
    rubrik_config = rubrik.get('v1', '/cluster/me', timeout=timeout)
    rubrik_tz = rubrik_config['timezone']['timezone']
    local_zone = pytz.timezone(rubrik_tz)
    utc_zone = pytz.timezone('utc')
    if not SINGLE_NODE:
        rubrik_cluster = get_rubrik_nodes(rubrik, user, password, token)
    else:
        rubrik_cluster.append({'session': rubrik, 'name': rubrik_config['name']})
    dprint(str(rubrik_cluster))
    if max_threads == 0:
        max_threads = thread_factor * len(rubrik_cluster)
    print("Using up to " + str(max_threads) + " threads across " + str(len(rubrik_cluster)) + " nodes.")
    if not physical:
        hs_data = rubrik.get('internal', '/host/share', timeout=timeout)
        for x in hs_data['data']:
            if x['hostname'] == host and x['exportPoint'] == share:
                share_id = x['id']
                break
        if share_id == "":
            sys.stderr.write("Share not found.\n")
            exit(2)
        fs_data = rubrik.get('v1', str('/fileset?share_id=' + share_id + '&name=fileset'), timeout=timeout)
    else:
        hs_data = rubrik.get('v1', '/host?name=' + host, timeout=timeout)
        share_id = str(hs_data['data'][0]['id'])
        os_type = str(hs_data['data'][0]['operatingSystemType'])
        dprint("OS_TYPE: " + os_type)
        if os_type == "Windows":
            delim = '\\'
        else:
            delim = '/'
        initial_path = '/'
        if share_id == "":
            sys.stderr.write("Host not found.\n")
            exit(2)
        fs_data = rubrik.get('v1', '/fileset?host_id=' + share_id, timeout=timeout)
    fs_id = ""
    for fs in fs_data['data']:
        if fs['name'] == fileset:
            fs_id = fs['id']
            break
    dprint("FS_ID: " + str(fs_id))
    snap_data = rubrik.get('v1', str('/fileset/' + fs_id), timeout=timeout)
    for snap in snap_data['snapshots']:
        s_time = snap['date']
        s_id = snap['id']
        s_time = s_time[:-5]
        snap_dt = datetime.datetime.strptime(s_time, '%Y-%m-%dT%H:%M:%S')
        snap_dt = pytz.utc.localize(snap_dt).astimezone(local_zone)
        snap_dt_s = snap_dt.strftime('%Y-%m-%d %H:%M:%S')
        snap_list.append((s_id, snap_dt_s))
    if latest:
        snap_index = len(snap_list) -1
        snap_id = snap_list[-1][0]
    elif date:
        dprint("TDATE: " + date_dt_s)
        for i, s in enumerate(snap_list):
            dprint(str(i) + ": " + s[1])
            if date_dt_s == s[1]:
                dprint("MATCH!")
                snap_index = i
                snap_id = snap_list[i][0]
    else:
        for i, snap in enumerate(snap_list):
            print(str(i) + ": " + snap[1] + "  [" + snap[0] + "]")
        valid = False
        while not valid:
            snap_index = python_input("Select Backup: ")
            try:
                snap_id = snap_list[int(snap_index)][0]
            except (IndexError, TypeError, ValueError) as e:
                print("Invalid Nase Index: " + str(e))
                continue
            valid = True
    print("Backup    : " + snap_list[int(snap_index)][1] + " [" + snap_id + "]")
    print("Compare to: " + local_path)
    if not latest and not date:
        go_s = python_input("Is this Correct? (y/n): ")
        if not go_s.startswith('Y') and not go_s.startswith('y'):
            exit(0)

