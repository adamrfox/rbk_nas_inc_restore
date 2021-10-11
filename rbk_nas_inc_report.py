#!/usr/bin/python

from __future__ import print_function
import rubrik_cdm
import sys
import os
import getopt
import getpass
import urllib3
urllib3.disable_warnings()
import datetime
import pytz
import time
import threading
try:
    import queue
except ImportError:
    import Queue as queue
import shutil
from random import randrange

class AtomicCounter:

    def __init__(self, initial=0):
        """Initialize a new atomic counter to given initial value (default 0)."""
        self.value = initial
        self._lock = threading.Lock()


    def increment(self, num=1):
        """Atomically increment the counter by num (default 1) and return the
        new value.
        """
        with self._lock:
            self.value += num
            return self.value

def python_input(message):
    if int(sys.version[0]) > 2:
        val = input(message)
    else:
        val = raw_input(message)
    return(val)

def walk_tree (rubrik, id, inc_date, delim, path, parent, files_to_restore, outfile):
    offset = 0
    done = False
    file_count = 0
    run_count.increment()
    job_path = path.split(delim)
    job_path_s = '_'.join(job_path)
    job_id = str(outfile) + str(job_path_s) + '.part'
    fh = open(job_id, "w")
    while not done:
        job_ptr = randrange(len(rubrik_cluster)-1)
        params = {"path": path, "offset": offset}
        if offset == 0:
            if VERBOSE:
                print("Starting job " + path + " on " + rubrik_cluster[job_ptr]['name'])
            else:
                print (' . ', end='')
        rbk_walk = rubrik.get('v1', '/fileset/snapshot/' + str(id) + '/browse', params=params, timeout=timeout)
        for dir_ent in rbk_walk['data']:
            offset += 1
            if dir_ent == parent:
                return
            if dir_ent['fileMode'] == "file":
                file_count += 1
                file_date_dt = datetime.datetime.strptime(dir_ent['lastModified'][:-5], "%Y-%m-%dT%H:%M:%S")
                file_date_epoch = (file_date_dt - datetime.datetime(1970, 1, 1)).total_seconds()
                if file_date_epoch > inc_date:
                    if path != delim:
#                        files_to_restore.append(path + delim + dir_ent['filename'])
                        oprint(path + delim + str(dir_ent['filename']) + "," + str(dir_ent['size']), fh)
                    else:
#                        files_to_restore.append(path + dir_ent['filename'])
                        oprint(path + str(dir_ent['filename']) + "," + str(dir_ent['size']), fh)
            elif dir_ent['fileMode'] == "directory" or dir_ent['fileMode'] == "drive":
                if dir_ent['fileMode'] == "drive":
                    new_path = dir_ent['filename']
                elif delim == "/":
                    if path == "/":
                        new_path = "/" + dir_ent['path']
                    else:
                        new_path = path + "/" + dir_ent['path']
                else:
                    if path == "\\":
                        new_path = "\\" + dir_ent['path']
                    else:
                        new_path = path + "\\" + dir_ent['path']
#                files_to_restore = walk_tree(rubrik, id, inc_date, delim, new_path, dir_ent, files_to_restore)
                job_queue.put(threading.Thread(name=new_path, target=walk_tree, args=(rubrik, id, inc_date, delim, new_path, dir_ent, files_to_restore, outfile)))
        if not rbk_walk['hasMore']:
            done = True
        else:
            run_count.increment(-1)
            print("JOB_DONE")
    fh.close()

def get_job_time(snap_list, id):
    time = ""
    dprint("JOB=" + id)
    for snap in snap_list:
        if snap[0] == id:
            time = snap[1]
            break
    return (time)

def dprint(message):
    if DEBUG:
        print(message + "\n")
    return()

def oprint(message, fh):
    if not fh:
        print(message)
    else:
        fh.write(message + "\n")

def log_clean(name):
    files = os.listdir('.')
    for f in files:
        if f.startswith(name) and f.endswith('.part'):
            os.remove(f)

def get_rubrik_nodes(rubrik, user, password, token):
    node_list = []
    cluster_network = rubrik.get('internal', '/cluster/me/network_interface')
    for n in cluster_network['data']:
        if n['interfaceType'] == "Management":
            if token:
                try:
                    rbk_session = rubrik_cdm.Connect(n['ipAddresses'][0], api_token=token)
                except Exception as e:
                    sys.stderr.write("Error on " + n['ipAddresses'][0] + ": " + str(e) + ".  Skipping\n")
                    continue
            else:
                try:
                    rbk_session = rubrik_cdm.Connect(n['ipAddresses'][0], user, password)
                except Exception as e:
                    sys.stderr.write("Error on " + n['ipAddresses'][0] + ": " + str(e) + ".  Skipping\n")
                    continue
            node_list.append({'session': rbk_session, 'name': n['nodeName']})
    return(node_list)

def usage():
    sys.stderr.write("Usage: rbk_nas_inc_report.py [-hDr] [-b backup] [-f fileset] [-c creds] rubrik\n")
    sys.stderr.write("-h | --help : Prints Usage\n")
    sys.stderr.write("-D | --debug : Debug mode.  Prints more information\n")
    sys.stderr.write("-o | --output : Specify an output file.  Only used with Report Mode\n")
    sys.stderr.write("-b | --backup : Specify a NAS backup.  Format is server:share\n")
    sys.stderr.write("-f | --fileset : Specify a fileset for the share\n")
    sys.stderr.write("-c | --creds : Specify cluster credentials.  Not secure.  Format is user:password\n")
    sys.stderr.write("-t | --token : Use an API token instead of credentials\n")
    sys.stderr.write("rubrik : Name or IP of the Rubrik Cluster\n")
    exit (0)


if __name__ == "__main__":
    backup = ""
    rubrik = ""
    user = ""
    password = ""
    fileset = ""
    date = ""
    share_id = ""
    restore_job = []
    snap_list = []
    restore_location = ""
    restore_share_id = ""
    restore_host_id = ""
    token = ""
    DEBUG = False
    VERBOSE = True
    REPORT_ONLY = True
    outfile = ""
    ofh = ""
    timeout = 300
    run_count = AtomicCounter()
    rubrik_cluster = []
    job_queue = queue.Queue()
    max_threads = 0
    debug_log = "debug_log.txt"
    large_trees = queue.Queue()
    SINGLE_NODE = False


    optlist, args = getopt.getopt(sys.argv[1:], 'b:f:c:d:hDst:o:m:v', ["backup=", "fileset=", "creds=", "date=", "help", "debug",  "token=", "output="])
    for opt, a in optlist:
        if opt in ("-b", "--backup"):
            backup = a
        if opt in ("-f", "--fileset"):
            fileset = a
        if opt in ("-c", "--creds"):
            user,password = a.split (":")
        if opt in ("-h", "--help"):
            usage()
        if opt in ("-d", "--date"):
            date = a
        if opt in ("-D", "--debug"):
            DEBUG = True
        if opt in ("-t", "--token"):
            token = a
        if opt in ("-o", "--outout"):
            outfile = a
        if opt in ('-s', '--single_node'):
            SINGLE_NODE = True
        if opt in ('-m', '--max_threads'):
            max_threads = int(a)
        if opt in ('-v', '--verbose'):
            VERBOSE = True
    try:
        rubrik_node = args[0]
    except:
        usage()
    if not outfile:
        usage()
    log_clean(outfile)
    if not backup:
        backup = python_input("Backup (host:share): ")
    if not fileset:
        fileset = python_input ("Fileset: ")
    if not token:
        if not user:
            user = python_input("User: ")
        if not password:
            password = getpass.getpass("Password: ")
    host, share = backup.split (":")
    if share.startswith("/"):
        delim = "/"
    else:
        delim = "\\"
#
# Find the latest snapshot for the share and  determine the date (2nd newest snap) or use the one provided by the user
#
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
    if max_threads == 0:
        max_threads = 10*len(rubrik_cluster)
    print("Using " + str(max_threads) + " threads across " + str(len(rubrik_cluster)) + " nodes.")
    hs_data = rubrik.get('internal', '/host/share', timeout=timeout)
    for x in hs_data['data']:
        if x['hostname'] == host and x['exportPoint'] == share:
            share_id = x['id']
            break
    if share_id == "":
        sys.stderr.write("Share not found\n")
        exit(2)
    fs_data = rubrik.get('v1', str("/fileset?share_id=" + share_id + "&name=" + fileset), timeout=timeout)
    fs_id = fs_data['data'][0]['id']
    dprint(fs_id)
    snap_data = rubrik.get('v1', str("/fileset/" + fs_id), timeout=timeout)
    for snap in snap_data['snapshots']:
        s_time = snap['date']
        s_id = snap['id']
        s_time = s_time[:-5]
        snap_dt = datetime.datetime.strptime(s_time, '%Y-%m-%dT%H:%M:%S')
        snap_dt = pytz.utc.localize(snap_dt).astimezone(local_zone)
        snap_dt_s = snap_dt.strftime('%Y-%m-%d %H:%M:%S')
        snap_list.append((s_id, snap_dt_s))
    for i, snap in enumerate(snap_list):
        print(str(i) + ": " + snap[1] + "  [" + snap[0] + "]")
    valid = False
    while not valid:
        start_index = python_input("Select Backup: ")
        try:
            start_id = snap_list[int(start_index)][0]
        except (IndexError, TypeError, ValueError) as e:
            print("Invalid Index: " + str(e))
            continue
        valid = True
    valid = False
    print("Backup: " + snap_list[int(start_index)][1] + " [" + start_id + "]")
    go_s = python_input("Is this correct? (y/n): ")
    if not go_s.startswith('Y') and not go_s.startswith('y'):
        exit (0)
    current_index = int(start_index)
    print("Gathering Incremental Data...")
    snap_info = rubrik.get('v1', '/fileset/snapshot/' + str(snap_list[current_index][0]), timeout=timeout)
    inc_date = datetime.datetime.strptime(snap_info['date'][:-5], "%Y-%m-%dT%H:%M:%S")
    inc_date_epoch = (inc_date - datetime.datetime(1970, 1, 1)).total_seconds()
    if current_index == 0:
        inc_date_epoch = 0
    else:
        snap_info = rubrik.get('v1', '/fileset/snapshot/' + str(snap_list[current_index-1][0]), timeout=timeout)
        inc_date = datetime.datetime.strptime(snap_info['date'][:-5], "%Y-%m-%dT%H:%M:%S")
        inc_date_epoch = (inc_date - datetime.datetime(1970, 1, 1)).total_seconds()
    files_to_restore = []
    dprint("INDEX: " + str(current_index) + "// DATE: " + str(inc_date_epoch))
    threading.Thread( name=outfile, target = walk_tree, args=(rubrik, snap_list[current_index][0], inc_date_epoch,
                                                                  delim, delim, {}, files_to_restore, outfile)).start()
    print("Waiting for jobs to queue")
    time.sleep(10)
    while not job_queue.empty() or (job_queue.empty and run_count.value > 0):
        if run_count.value < max_threads and not job_queue.empty():
            job = job_queue.get()
            print("\nQueue: " + str(job_queue.qsize()))
            print("Running Threads: " + str(run_count.value))
            job.start()
        elif not job_queue.empty():
            time.sleep(10)
            print("\nQueue: " + str(job_queue.qsize()))
            print("Running Threads: " + str(run_count.value))
            print("Q: " + str(list(job_queue.queue)))
        else:
            print("\nWaiting on " + str(run_count.value) + " jobs to finish.")
            time.sleep(10)
    print("done")
