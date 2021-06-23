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
        params = {"path": path, "offset": offset}
        rbk_walk = rubrik.get('v1', '/fileset/snapshot/' + str(id) + '/browse', params=params)
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
    REPORT_ONLY = True
    outfile = ""
    ofh = ""

    optlist, args = getopt.getopt(sys.argv[1:], 'b:f:c:d:hDt:o:', ["backup=", "fileset=", "creds=", "date=", "help", "debug",  "token=", "output="])
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
    try:
        rubrik_node = args[0]
    except:
        usage()
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
    rubrik_config = rubrik.get('v1', '/cluster/me')
    rubrik_tz = rubrik_config['timezone']['timezone']
    local_zone = pytz.timezone(rubrik_tz)
    utc_zone = pytz.timezone('utc')
    hs_data = rubrik.get('internal', '/host/share')
    for x in hs_data['data']:
        if x['hostname'] == host and x['exportPoint'] == share:
            share_id = x['id']
            break
    if share_id == "":
        sys.stderr.write("Share not found\n")
        exit(2)
    fs_data = rubrik.get('v1', str("/fileset?share_id=" + share_id + "&name=" + fileset))
    fs_id = fs_data['data'][0]['id']
    dprint(fs_id)
    snap_data = rubrik.get('v1', str("/fileset/" + fs_id))
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
    print("Start: " + snap_list[int(start_index)][1] + " [" + start_id + "]")
    print("Last: " + snap_list[int(end_index)][1] + " [" + end_id + "]")
    go_s = python_input("Is this correct? (y/n): ")
    if not go_s.startswith('Y') and not go_s.startswith('y'):
        exit (0)
    current_index = int(start_index)
    print("Gathering Incremental Data...")
    snap_info = rubrik.get('v1', '/fileset/snapshot/' + str(snap_list[current_index][0]))
    inc_date = datetime.datetime.strptime(snap_info['date'][:-5], "%Y-%m-%dT%H:%M:%S")
    inc_date_epoch = (inc_date - datetime.datetime(1970, 1, 1)).total_seconds()
    if current_index == 0:
        inc_date_epoch = 0
    else:
        snap_info = rubrik.get('v1', '/fileset/snapshot/' + str(snap_list[current_index-1][0]))
        inc_date = datetime.datetime.strptime(snap_info['date'][:-5], "%Y-%m-%dT%H:%M:%S")
        inc_date_epoch = (inc_date - datetime.datetime(1970, 1, 1)).total_seconds()
    if outfile:
        ofh = open(outfile, "w")
    while current_index <= int(end_index):
        files_to_restore = []
        dprint("INDEX: " + str(current_index) + "// DATE: " + str(inc_date_epoch))
        files_to_restore = walk_tree(rubrik, snap_list[current_index][0], inc_date_epoch, delim, delim, {}, files_to_restore)
        oprint ("FILES in " + str(snap_list[current_index][0]) + " [" + str(snap_list[current_index][1]) + "]", ofh)
        for f in files_to_restore:
            oprint("    " + str(f), ofh)
        oprint ("-----------------", ofh)
        if current_index <= int(end_index):
            snap_info = rubrik.get('v1', '/fileset/snapshot/' + str(snap_list[current_index][0]))
            inc_date = datetime.datetime.strptime(snap_info['date'][:-5], "%Y-%m-%dT%H:%M:%S")
            inc_date_epoch = (inc_date - datetime.datetime(1970, 1, 1)).total_seconds()
        current_index += 1


