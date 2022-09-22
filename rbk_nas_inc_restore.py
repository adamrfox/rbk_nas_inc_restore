#!/usr/bin/python

from __future__ import print_function
import rubrik_cdm
import sys
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
from random import randrange
from pprint import pprint


def python_input(message):
    if int(sys.version[0]) > 2:
        val = input(message)
    else:
        val = raw_input(message)
    return(val)

def walk_tree (rubrik, id, inc_date, delim, path, parent, files_to_restore):
    offset = 0
    done = False
    while not done:
        job_ptr = randrange(len(rubrik_cluster))
        params = {"path": path, "offset": offset}
        if offset == 0:
            if VERBOSE:
                dprint("Scanning " + path + " on " + rubrik_cluster[job_ptr]['name'])
            else:
                print(' . ', end='')
        rbk_walk = rubrik.get('v1', '/fileset/snapshot/' + str(id) + '/browse', params=params, timeout=timeout)
        for dir_ent in rbk_walk['data']:
            offset += 1
            if dir_ent == parent:
                return (files_to_restore)
            if dir_ent['fileMode'] == "file":
                file_date_dt = datetime.datetime.strptime(dir_ent['lastModified'][:-5], "%Y-%m-%dT%H:%M:%S")
                file_date_epoch = (file_date_dt - datetime.datetime(1970, 1, 1)).total_seconds()
                if file_date_epoch > inc_date:
                    if path != delim:
                        files_to_restore.append(path + delim + dir_ent['filename'])
                    else:
                        files_to_restore.append(path + dir_ent['filename'])
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
                files_to_restore = walk_tree(rubrik, id, inc_date, delim, new_path, dir_ent, files_to_restore)
        if not rbk_walk['hasMore']:
            done = True
    return (files_to_restore)

def run_restore(type, rubrik, snap_id, restore_config):
    if type == "restore_files" or type == "export_files":
        rubrik_restore = rubrik.post('internal', '/fileset/snapshot/' + str(snap_id) + "/" + type, restore_config)
    else:
        rubrik_restore = rubrik.post('v1', '/fileset/snapshot/' + str(snap_id) + "/" + type, restore_config)
    job_status_url = str(rubrik_restore['links'][0]['href']).split('/')
    job_status_path = "/" + "/".join(job_status_url[5:])
    done = False
    while not done:
        restore_job_status = rubrik.get('v1', job_status_path, timeout=timeout)
        job_status = restore_job_status['status']
        if job_status in ['RUNNING', 'QUEUED', 'ACQUIRING', 'FINISHING']:
            print("Progress: " + str(restore_job_status['progress']) + "%")
            time.sleep(5)
        elif job_status == "SUCCEEDED":
            print("Done")
            done = True
        elif job_status == "TO_CANCEL" or 'endTime' in job_status:
            sys.stderr.write("Job ended with status: " + job_status + "\n")
            exit(1)
        else:
            print("Status: " + job_status)
    return()

def generate_restore_config(files, restore_type, restore_path, delim):
    file_list = []
    for f in files:
        ff = f.split(delim)
        ff.pop()
        file_path = delim.join(ff)
        if restore_path == delim:
            if file_path[-1] == delim:
                restore_path_instance = file_path[:-1]
            else:
                restore_path_instance = file_path
        else:
            restore_path_instance = restore_path + file_path
        dprint("Instance = " + restore_path_instance)
        if restore_type == "restore_files":
            file_config = {"path": f, "restorePath": restore_path_instance}
        else:
            file_config = {"srcPath": f, "dstPath": restore_path_instance}
        file_list.append(file_config)
    if restore_type == "restore_files":
        res_config = {"restoreConfig": file_list, "ignoreErrors": True}
    else:
        res_config = {"exportPathPairs": file_list, "ignoreErrors": True}
    return(res_config)

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
        dfh = open(debug_log, "a")
        dfh.write(message + "\n")
        dfh.close()
    return()

def oprint(message, fh):
    if not fh:
        print(message)
    else:
        fh.write(message + "\n")

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
            try:
                node_list.append({'session': rbk_session, 'name': n['nodeName']})
            except KeyError:
                node_list.append({'session': rbk_session, 'name': n['node']})
    return(node_list)


def usage():
    sys.stderr.write("Usage: rbk_nas_inc_restore.py [-hDr] [-b backup] [-f fileset] [-c creds] rubrik\n")
    sys.stderr.write("-h | --help : Prints Usage\n")
    sys.stderr.write("-D | --debug : Debug mode.  Prints more information\n")
    sys.stderr.write("-r | --report : Report Mode.  Do not restore, rather show files in the selected backups\n")
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
    VERBOSE = False
    export_flag = False
    REPORT_ONLY = False
    outfile = ""
    ofh = ""
    timeout = 300
    debug_log = "debug_log.txt"
    job_queue = queue.Queue()
    large_trees = queue.Queue()
    files_to_restore = queue.Queue()
    SINGLE_NODE = False
    physical = False
    max_threads = 0
    rubrik_cluster = []


    optlist, args = getopt.getopt(sys.argv[1:], 'b:f:c:d:hDrt:o:m:spv', ["backup=", "fileset=", "creds=", "date=", "help",
                                                                        "debug", "report", "token=", "output=", '--max_threads=',
                                                                        '--singele_node', '--physical', '--verbose'])
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
            VERBOSE = True
            dfh = open(debug_log, "w")
            dfh.close()
        if opt in ("-r", "--report"):
            REPORT_ONLY = True
        if opt in ("-t", "--token"):
            token = a
        if opt in ("-o", "--outout"):
            outfile = a
        if opt in ("-m", "--max_threads"):
            max_threads = int(a)
        if opt in ("-s", '--single_node'):
            SINGLE_NODE = True
        if opt in ('-p', '--physical'):
            physical = True
        if opt in ('-v', '--verbose'):
            VERBOSE = True

    try:
        rubrik_node = args[0]
    except:
        usage()
    if not backup:
        if not physical:
            backup = python_input("Backup (host:share): ")
        else:
            backup = python_input("Host: ")
    if not physical:
        host, share = backup.split(':')
    else:
        host = backup
    if not fileset:
        fileset = python_input ("Fileset: ")
    if not token:
        if not user:
            user = python_input("User: ")
        if not password:
            password = getpass.getpass("Password: ")
    if not physical:
        if share.startswith("/"):
            delim = "/"
        else:
            delim = "\\"
        initial_path = delim
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
    dprint(str(rubrik_cluster))
    if max_threads == 0:
        max_threads = 10*len(rubrik_cluster)
    print("Using up to " + str(max_threads) + " threads across " + str(len(rubrik_cluster)) + " nodes")
    if not physical:
        hs_data = rubrik.get('internal', '/host/share', timeout=timeout)
        for x in hs_data['data']:
            if x['hostname'] == host and x['exportPoint'] == share:
                share_id = x['id']
                break
        if share_id == "":
            sys.stderr.write("Share not found\n")
            exit(2)
        fs_data = rubrik.get('v1', str("/fileset?share_id=" + share_id + "&name=" + fileset), timeout=timeout)
    else:
        hs_data = rubrik.get('v1', '/host?name=' + host, timeout=timeout)
        share_id = str(hs_data['data'][0]['id'])
        os_type = str(hs_data['data'][0]['operatingSystemType'])
        dprint("OS_TYPE: " + os_type)
        if os_type == "Windows":
            delim = "\\"
        else:
            delim = "/"
        initial_path = "/"
        if share_id == "":
            sys.stderr.write("Host not found\n")
            exit(2)
        fs_data = rubrik.get('v1', '/fileset?host_id=' + share_id, timeout=timeout)
    fs_id = ""
    for fs in fs_data['data']:
        if fs['name'] == fileset:
            fs_id = fs['id']
            break
    dprint("FS_ID: " + fs_id)
    if fs_id == "":
        sys.stderr.write("Can't find fileset: " + fileset + '\n')
        exit(2)
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
        start_index = python_input("Starting backup: ")
        try:
            start_id = snap_list[int(start_index)][0]
        except (IndexError, TypeError, ValueError) as e:
            print("Invalid Index: " + str(e))
            continue
        valid = True
    valid = False
    while not valid:
        end_index = python_input("Last backup: ")
        try:
            end_id = snap_list[int(end_index)][0]
        except (IndexError, TypeError, ValueError) as e:
            print("Invalid Index: " + str(e))
            continue
        if end_index < start_index:
            print("Last snap must be later than the first")
            continue
        valid = True
    do_full = False
    if not REPORT_ONLY:
        do_full_s = python_input("Use starting backup as first full (y/n): ")
        if do_full_s.startswith('Y') or do_full_s.startswith('y'):
            do_full = True
        valid = False
        while not valid:
            restore_location = python_input("Restore Location: ")
            if ':' in restore_location:
                try:
                    (restore_host, restore_share, restore_path) = restore_location.split(':')
                except ValueError as e:
                    print(str(e))
                    print("Export format is host:share:path")
                    continue
                hs_data = rubrik.get('internal', '/host/share', timeout=timeout)
                for x in hs_data['data']:
                    if x['hostname'] == restore_host and x['exportPoint'] == restore_share:
                        restore_share_id = x['id']
                        restore_host_id = x['hostId']
                        export_flag = True
                        valid = True
                        break
                if restore_share_id == "":
                    sys.stderr.write("Restore Share Not Found.\n")
            else:
                restore_host = host
                if not physical:
                    restore_share = share
                if restore_location == "":
                    restore_path = delim
                else:
                    restore_path = restore_location
                valid = True
    print("Start: " + snap_list[int(start_index)][1] + " [" + start_id + "]")
    print("Last: " + snap_list[int(end_index)][1] + " [" + end_id + "]")
    if not REPORT_ONLY:
        if do_full:
            print("Use Starting backup as a full backup")
        else:
            print("Use Starting backup as baseline for incrementals")
        if restore_location == "":
            print("Restore to: Original Location")
        else:
            print("Restore to: " + restore_host + ":" + restore_share + ":" + restore_path)
    go_s = python_input("Is this correct? (y/n): ")
    if not go_s.startswith('Y') and not go_s.startswith('y'):
        exit (0)
    current_index = int(start_index)
    if do_full:
        if restore_host == host and restore_share == share:
            print("In-place Full Restore from  " + snap_list[int(start_index)][1])
            restore_config = {"sourceDir": restore_path, "destinationDir": restore_path, "ignoreErrors": True}
            run_restore("restore_file", rubrik, start_id, restore_config)
        else:
            print("Full Restore from " + snap_list[int(start_index)][1] + " to " + restore_host + ":" + restore_path)
            restore_config = {"sourceDir": delim, "destinationDir": restore_path, "ingoreErrors": True, "hostId": restore_host_id, "shareId": restore_share_id}
            run_restore("export_file", rubrik, start_id, restore_config)
    print("Gathering Incremental Data...")
    snap_info = rubrik.get('v1', '/fileset/snapshot/' + str(snap_list[current_index][0]), timeout=timeout)
    inc_date = datetime.datetime.strptime(snap_info['date'][:-5], "%Y-%m-%dT%H:%M:%S")
    inc_date_epoch = (inc_date - datetime.datetime(1970, 1, 1)).total_seconds()
    if not REPORT_ONLY:
        current_index += 1
    else:
        if current_index == 0:
            inc_date_epoch = 0
        else:
            snap_info = rubrik.get('v1', '/fileset/snapshot/' + str(snap_list[current_index-1][0]), timeout=timeout)
            inc_date = datetime.datetime.strptime(snap_info['date'][:-5], "%Y-%m-%dT%H:%M:%S")
            inc_date_epoch = (inc_date - datetime.datetime(1970, 1, 1)).total_seconds()
        if outfile:
            ofh = open(outfile, "w")
    while current_index <= int(end_index):
        dprint("INDEX: " + str(current_index) + "// DATE: " + str(inc_date_epoch))
        files_to_restore = walk_tree(rubrik_cluster, snap_list[current_index][0], inc_date_epoch, delim, initial_path, {}, files_to_restore)
        if REPORT_ONLY:
            oprint ("FILES in " + str(snap_list[current_index][0]) + " [" + str(snap_list[current_index][1]) + "]", ofh)
            for f in files_to_restore:
                oprint("    " + str(f), ofh)
            oprint ("-----------------", ofh)
        else:
            restore_job.append((snap_list[current_index][0], files_to_restore))
        if current_index <= int(end_index):
            snap_info = rubrik.get('v1', '/fileset/snapshot/' + str(snap_list[current_index][0]), timeout=timeout)
            inc_date = datetime.datetime.strptime(snap_info['date'][:-5], "%Y-%m-%dT%H:%M:%S")
            inc_date_epoch = (inc_date - datetime.datetime(1970, 1, 1)).total_seconds()
        current_index += 1
    dprint(str(restore_job))
    x = 0
    if not REPORT_ONLY:
        print("Reducing Incremental Data...")
        while x < len(restore_job)-1:
            delete_list = []
            for i, f in enumerate(restore_job[x][1]):
                y = x + 1
                while y < len(restore_job):
                    if f in restore_job[y][1]:
                        delete_list.append(f)
                    y += 1
            for dc in delete_list:
                restore_job[x][1].remove(dc)
            x += 1
        dprint(str(restore_job))
        for job in restore_job:
            if job[1]:
                if export_flag:
                    job_type = "export_files"
                    restore_config = generate_restore_config(job[1], job_type, restore_path, delim)
                    restore_config.update({"hostId": restore_host_id, "shareId": restore_share_id})
                else:
                    job_type = "restore_files"
                    restore_config = generate_restore_config(job[1], job_type, restore_path, delim)
                dprint(job[0] + ":")
                dprint(str(restore_config))
                job_time = get_job_time(snap_list, job[0])
                print("Incremental Restore from " + job_time + " to " + restore_host + " : " + restore_path)
                run_restore(job_type, rubrik, job[0], restore_config)


