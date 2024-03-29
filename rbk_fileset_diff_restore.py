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
            list_check.append(thread.name)
    dprint("JQD returns " + str(len(list_check)))
    return(len(list_check))

def file_compare_new(files_in_base_dir, local_path, files_to_restore):
    for f in files_in_base_dir.keys():
        if local_path:
            lf = local_path + f
        else:
            lf = f
        try:
            f_stat = os.stat(lf)
        except FileNotFoundError:
            files_to_restore.put({'name': f, 'size': files_in_base_dir[f]['size']})
            continue
#        print(f + ": LOCAL: " + str(int(f_stat.st_mtime)) + " // " + str(int(files_in_base_dir[f]['time'])))
        if f_stat.st_mtime < files_in_base_dir[f]['time']:
            files_to_restore.put({'name': f, 'size': files_in_base_dir[f]['size']})
        elif OVERWRITE_NEW and int(f_stat.st_mtime) > int(files_in_base_dir[f]['time']):
            files_to_restore.put({'name': f, 'size': files_in_base_dir[f]['size']})


def dir_has_no_files(job_ptr, new_path, id):
    params = {'path': new_path, 'offset': 0}
    dir_chk = rubrik_cluster[job_ptr]['session'].get('v1', '/fileset/snapshot/' + str(id) + '/browse', params=params,
                                                        timeout=timeout)
    return(len(dir_chk['data']) == 0)


def walk_tree(rubrik, id, local_path, delim, path, parent, files_to_restore):
    offset = 0
    done = False
    file_count = 0
    files_in_base_dir = {}
    while not done:
        job_ptr = randrange(len(rubrik_cluster))
        params = {'path': path, "offset": offset}
        if offset == 0:
            if VERBOSE:
                print("Starting job " + path + " on " + rubrik_cluster[job_ptr]['name'])
            else:
                print (' . ', end='')
        rbk_walk = rubrik_cluster[job_ptr]['session'].get('v1', '/fileset/snapshot/' + str(id) + "/browse",
                                                          params=params, timeout=timeout)
        file_count = 0
        for dir_ent in rbk_walk['data']:
            offset += 1
            file_count += 1
            if dir_ent == parent:
                return
            if dir_ent['fileMode'] == "directory" or dir_ent['fileMode'] == "drive":
                if dir_ent['fileMode'] == "drive":
                    new_path = dir_ent['filename']
                elif delim == "/":
                    if path == "/":
                        new_path = "/" + dir_ent['path']
                    else:
                        new_path = path + '/' + dir_ent['path']
                else:
                    if path == "\\":
                        new_path = "\\" + dir_ent['path']
                    else:
                        new_path = path + '\\' + dir_ent['path']
                if local_path:
                    local_new_path = local_path + new_path
                else:
                    local_new_path = new_path
                try:
                    os.stat(local_new_path)
                except FileNotFoundError:
                    if dir_has_no_files(job_ptr, new_path, id):
                        files_to_restore.put({'name': new_path, 'size': 0})
                        continue
                job_queue.put(threading.Thread(name=new_path, target=walk_tree, args=(rubrik, id, local_path, delim,
                                                                                    new_path, dir_ent, files_to_restore)))
            else:
                mt_s = datetime.datetime.strptime(dir_ent['lastModified'][:-5], '%Y-%m-%dT%H:%M:%S')
                mt = (mt_s - datetime.datetime(1970, 1, 1)).total_seconds()
                if path == delim:
                    files_in_base_dir[delim + str(dir_ent['filename'])] = {'size': dir_ent['size'], 'time': mt}
                else:
                    files_in_base_dir[path + delim + str(dir_ent['filename'])] = {'size': dir_ent['size'],
                    'time': mt}
        if not rbk_walk['hasMore']:
            done = True
        else:
            dprint("HASMORE: " + str(offset))
#    print("FILES_IN_BD: "+ str(files_in_base_dir))
    file_compare_new(files_in_base_dir, local_path, files_to_restore)
    if file_count == 200000:
        large_trees.put(path)


def build_restore_job(files, path, max_files):
    files_list = []
    while files.qsize():
        if max_files > 0 and len(files_list) >= max_files:
            break
        f = files.get()
        rpf = f['name'].split(delim)
        rpf.pop()
        rpath = delim.join(rpf)
        if rpath == "":
            rpath = delim
        files_list.append({'path': f['name'], 'restorePath': rpath})
    res_cfg = {'restoreConfig': files_list, 'ignoreErrors': True}
    return(res_cfg)


def usage():
    sys.stderr.write("Usage: rbk_fileset_diff_restore.py [-hDsvlor] [-c creds] [-t token] [-b backup] [-f fileset] [-d date] [-m threads] [-M thread_factor] [-F files_per_restore] rubrik [local_path]\n")
    sys.stderr.write("-h | --help : Prints Usage\n")
    sys.stderr.write("-D | --DEBUG : Generate debugging information\n")
    sys.stderr.write("-s | --single_node : Only use 1 Rubrik node\n")
    sys.stderr.write("-v | --verbose : Verbose output\n")
    sys.stderr.write("-l | --latest : Use the latest backup\n")
    sys.stderr.write("-o | --overwrite : Overwrite newer files on source\n")
    sys.stderr.write("-r | --report_only : Only show what would be restored.  No restore done\n")
    sys.stderr.write("-c | --creds : Rubrik credentials [user:password].  Note: Does not work with MFA\n")
    sys.stderr.write("-t | --token : Rubrik API Token\n")
    sys.stderr.write("-b | --backup : Specify the Rubrik Backup [host for physical, host:share for NAS]\n")
    sys.stderr.write("-f | --fileset : Specify the fileset associated with the host or share\n")
    sys.stderr.write("-d | --date : Specify a specfic date/time stamp for the backup [%Y-%m-%dT%H:%M:%S]\n")
    sys.stderr.write("-m | --max_threads : Specify the maximum number of scan threads [def: 10*nodes]\n")
    sys.stderr.write("-M | --thread_factor : Specify the maximum number of scan threads by number of nodes [def: 10]\n")
    sys.stderr.write("-F | --files_per_job : Specify the maximum number of files per restore job [def: " + str(FILES_PER_RESTORE_JOB) + "]\n")
    sys.stderr.write("rubrik : Name or IP of a Rubrik node\n")
    sys.stderr.write("local_path : Local path of the data on the host [NAS only]\n")
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
    OVERWRITE_NEW = False
    ofh = ""
    timeout = 360
    rubrik_cluster = []
    job_queue = queue.Queue()
    max_threads = 0
    thread_factor = 10
    debug_log = "debug_log.txt"
    large_trees = queue.Queue()
    files_to_restore = queue.Queue()
    SINGLE_NODE = False
    thread_list = []
    restore_config = {}
    FILES_PER_RESTORE_JOB = 15000
    files_in_directory = {}

    (optlist, args) = getopt.getopt(sys.argv[1:], 'b:f:c:hd:Dt:sm:M:vlorF:', ['backup=', 'fileset=', 'creds=', 'date=',
                                                                          'help', 'DEBUG', 'token=', 'max_threads=',
                                                                          'thread_factor=', 'single_node', 'verbose',
                                                                          'latest', '--overwrite', 'report_only',
                                                                              'files_per_job='])
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
            date_dt_s = datetime.datetime.strftime(date_dt, "%Y-%m-%dT%H:%M:%S")
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
        if opt in ('-o', '--overwrite'):
            OVERWRITE_NEW = True
        if opt in ('-r', '--report_only'):
            REPORT_ONLY = True
        if opt in ('-F', '--files_per_job'):
            FILES_PER_RESTORE_JOB = int(a)

    try:
        rubrik_node = args[0]
    except:
        dprint("Usage Called.  No Rubrik Node/local path")
        usage()
    try:
        local_path = args[1]
    except:
        local_path = ""
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
        fs_data = rubrik.get('v1', str('/fileset?share_id=' + share_id), timeout=timeout)
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
    dprint("FS_DATA: " + str(fs_data))
    fs_id = ""
    for fs in fs_data['data']:
        if fs['name'] == fileset:
            fs_id = fs['id']
            break
    dprint("FS_ID: " + str(fs_id))
    if fs_id == "":
        sys.stderr.write("Fileset not found: " + fileset + "\n")
        exit(2)
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
                print("Invalid Base Index: " + str(e))
                continue
            valid = True
    print("Backup    : " + snap_list[int(snap_index)][1] + " [" + snap_id + "]")
    if local_path:
        print("Compare to: " + local_path)
    else:
        print("Compare to: local path")
    if not latest and not date:
        go_s = python_input("Is this Correct? (y/n): ")
        if not go_s.startswith('Y') and not go_s.startswith('y'):
            exit(0)

    threading.Thread(name='root', target=walk_tree, args=(rubrik, snap_list[int(snap_index)][0], local_path, delim,
                                                          initial_path, {}, files_to_restore)).start()
    thread_list.append('root')
    print("Waiting for jobs to queue")
    time.sleep(20)
    first = True
    while first or (not job_queue.empty() or job_queue_length(thread_list)):
        first = False
        jql = job_queue_length(thread_list)
        if jql < max_threads and not job_queue.empty():
            #            dprint(str(list(job_queue.queue)))
            job = job_queue.get()
            print("\nQueue: " + str(job_queue.qsize()))
            print("Running Threads: " + str(jql))
            dprint("Started job: " + str(job))
            job.start()
            thread_list.append(job.name)
        elif not job_queue.empty():
            time.sleep(10)
            print("\nQueue: " + str(job_queue.qsize()))
            print("Running Threads: " + str(jql))
        else:
            if DEBUG:
                dprint(str(threading.active_count()) + " running:")
                for t in threading.enumerate():
                    dprint("\t " + str(t.name))
                dprint('\n')
            if jql > 0:
                print("\nWaiting on " + str(jql-1) + " jobs to finish.")
            time.sleep(10)
        dprint(str(list(job_queue.queue)))
        dprint(str(thread_list))
    if not large_trees.empty():
        print("NOTE: There is an default API browse limit of 200K files per directory.")
        print("The following directories could have more than 200K files:")
        for d in large_trees.queue:
            print(d)
        print("\nThis value can be raised by Rubrik Support. If you need this, open a case with Rubrik")
    total_files_to_restore = str(files_to_restore.qsize())
    if REPORT_ONLY or VERBOSE:
        print("Files to Restore (" + str(total_files_to_restore) + "):")
        for f in files_to_restore.queue:
            print(f['name'] + ',' + str(f['size']))
        if REPORT_ONLY:
            exit(0)
    print('\nTotal files to restore: ' + str(total_files_to_restore))
    while files_to_restore.qsize():
        restore_config = build_restore_job(files_to_restore, local_path, FILES_PER_RESTORE_JOB)
        dprint("RESTORE_CONFIG:")
        dprint(str(restore_config))
        if not REPORT_ONLY:
            print("Restoring " + str(len(restore_config['restoreConfig'])) + " files")
            rubrik_restore = rubrik.post('internal', '/fileset/snapshot/' + str(snap_id) + "/restore_files", restore_config)
            job_status_url = str(rubrik_restore['links'][0]['href']).split('/')
            job_status_path = "/" + "/".join(job_status_url[5:])
            done = False
            first = True
            while not done:
                restore_job_status = rubrik.get('v1', job_status_path, timeout=timeout)
                job_status = restore_job_status['status']
                if job_status in ['RUNNING', 'QUEUED', 'ACQUIRING', 'FINISHING']:
                    if first:
                        first = False
                        print("\nProgress: " + str(restore_job_status['progress']) + "%", end='')
                    else:
                        for i in enumerate(range(plen + 1)):
                            print('\b', end='')
                        print(str(restore_job_status['progress']) + "%", end='')
                        sys.stdout.flush()
                    plen = len(str(restore_job_status['progress']))
                    time.sleep(5)
                elif job_status == "SUCCEEDED":
                    print("\nDone")
                    done = True
                elif job_status == "TO_CANCEL" or 'endTime' in job_status:
                    sys.stderr.write("\nJob ended with status: " + job_status + "\n")
                    exit(1)
                else:
                    print("Status: " + job_status)
            print("Files Remaining to Restore: " + str(files_to_restore.qsize()))
