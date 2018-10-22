#!/usr/bin/env python
"""

Tools to summarise access-om2 runs.
Also includes a command-line interface.

Latest version: https://github.com/aekiss/run_summary
Author: Andrew Kiss https://github.com/aekiss
Apache 2.0 License http://www.apache.org/licenses/LICENSE-2.0.txt
"""
# import copy
# import time
# import datetime
import re
import glob
import subprocess
import datetime

def parse_pbs_log(fname):
    """
    Returns dict of items from parsed PBS log file.

    fname: PBS log file path

    output: dict

    example of PBS log file content to parse:
        git commit -am "2018-10-08 22:32:26: Run 137"
        TODO: Check if commit is unchanged
        ======================================================================================
                          Resource Usage on 2018-10-08 22:32:36:
           Job Id:             949753.r-man2
           Project:            x77
           Exit Status:        0
           Service Units:      20440.40
           NCPUs Requested:    5968                   NCPUs Used: 5968
                                                   CPU Time Used: 20196:31:07
           Memory Requested:   11.66TB               Memory Used: 2.61TB
           Walltime requested: 05:00:00            Walltime Used: 03:25:30
           JobFS requested:    36.43GB                JobFS used: 1.0KB
        ======================================================================================

    """
    def null(l):
        return l[1]

    def getrun(l):
        return int(l[4].rstrip('"'))

    def getjob(l):
        return int(l[1].split('.')[0])

    def getint(l):
        return int(l[1])

    def getfloat(l):
        return float(l[1])

    def getsec(l):  # convert hh:mm:ss to sec
        return sum(x * int(t) for x, t in zip([3600, 60, 1], l[1].split(':')))

    def getdatetime(l):  # BUG: doesn't include time zone (can't tell if we're on daylight savings time)
        return l[0]+'T'+l[1].rstrip(':')

    def getbytes(l):  # assumes PBS log info uses binary prefixes - TODO: check
        s = l[1]
        m = re.match('((\.|\d)+)', s)
        n = float(m.group(0))  # numerical part
        units = {'B': 1,
                 'KB': 2**10,
                 'MB': 2**20,
                 'GB': 2**30,
                 'TB': 2**40}
        return int(round(n*units[s[m.end(0):]]))

    search_items = {  # keys are strings to search for; items are functions to apply to whitespace-delimited list of strings following key
        'git commit': getrun,
        'Resource Usage on': getdatetime,
        'Job Id': getjob,
        'Project': null,
        'Exit Status': getint,
        'Service Units': getfloat,
        'NCPUs Requested': getint,
        'NCPUs Used': getint,
        'CPU Time Used': getsec,
        'Memory Requested': getbytes,
        'Memory Used': getbytes,
        'Walltime requested': getsec,
        'Walltime Used': getsec,
        'JobFS requested': getbytes,
        'JobFS used': getbytes}
    parsed_items = search_items.fromkeys(search_items, None)

    with open(fname, 'r') as infile:
        for line in infile:
            #  NB: subsequent matches will replace earlier ones
            for key, op in search_items.items():
                try:
                    parsed_items[key] = op(line.split(key)[1].split())
                except:
                    continue

    # change to more self-explanatory keys
    rename_keys = {'git commit': 'Run number',
                   'Resource Usage on': 'Run completion date'}
    for oldkey, newkey in rename_keys.items():
        parsed_items[newkey] = parsed_items.pop(oldkey)

    return parsed_items

# parse_pbs_log('test/01deg_jra55_iaf.o1399186')

# print(parse_pbs_log('archive/pbs_logs/01deg_jra55_iaf.o678614'))

def parse_git_log(datestr):
    """
    Returns dict of items from git log on given date.

    datestr: date string

    output: dict

    example of git log content to parse:
        commit 4822bfeef6c5649b4f470906c44f8427ed9e4151
        Author: Andrew Kiss <aek156@r358.(none)>
        Date:   Fri Sep 14 04:55:09 2018 +1000

            2018-09-14 04:55:09: Run 92
    """
    # possible BUG: what time zone flag should be use? local is problematic if run from overseas....?
    # BUG: this won't work - result is status not stdout
    # TODO: use git log --format: to get what we want in an easily-parsed format
    # log = run('git log -1 `git rev-list -1 --date=local --before="'
              # + datestr + '" HEAD`'.split())
    # p = subprocess.Popen('git log -1 --format:"%H%n%an%n%aI" `git rev-list -1 --date=local --before="'
    #                    + datestr + '" HEAD`', stdout=subprocess.PIPE, shell=True)
    # use Popen for backwards-compatiblity with Python <2.7
    p = subprocess.Popen('git log -1 --pretty="format:%H%x09%an%x09%aI" '
                         + '`git rev-list -1 --date=local --before="'
                         + datestr + '" HEAD`', stdout=subprocess.PIPE, shell=True)
    log = p.communicate()[0].decode('ascii').encode('ascii').split('\t')
    # print(log)
    parsed_items = dict()
    parsed_items['Commit'] = log[0]
    parsed_items['Author'] = log[1]
    parsed_items['Date'] = log[2]
    # print(parsed_items)
    return parsed_items

# print(parse_git_log('2018-09-14T04:55:22'))

def parse_mom_time_stamp(run):
    """
    Returns dict of items from parsed MOM time_stamp.out.

    run: run number

    output: dict

    example of MOM time_stamp.out content to parse:
        2001   9   1   0   0   0  Sep
        2001  11   1   0   0   0  Nov

    """
    parsed_items = {'Model start time': None,
                    'Model end time': None,
                    'Time stamp file': None}
    fname = 'archive/output' + str(run).zfill(3) + '/ocean/time_stamp.out'
    parsed_items['Time stamp file'] = fname
    with open(fname, 'r') as infile:
        line = infile.readline()
        parsed_items['Model start time'] = datetime.datetime(
            *list(map(int, line.split()[0:-1]))).isoformat()
        line = infile.readline()
        parsed_items['Model end time'] = datetime.datetime(
            *list(map(int, line.split()[0:-1]))).isoformat()
    return parsed_items


run_data = dict()
for f in glob.glob('archive/pbs_logs/01deg_jra55_iaf.o*'):
    jobid = int(f.split('.o')[1])
    run_data[jobid] = dict()
    run_data[jobid]['PBS log'] = parse_pbs_log(f)
    run_data[jobid]['PBS log']['PBS log file'] = f

# print(run_data)

for jobid in run_data:
    pbs = run_data[jobid]['PBS log']
    date = pbs['Run completion date']
    if date is not None:
        run_data[jobid]['git log'] = parse_git_log(date)  # BUG: assumes the time zones match
        if pbs['Exit Status'] == 0:
            run_data[jobid]['MOM_time_stamp.out'] = \
                parse_mom_time_stamp(pbs['Run number'])

print(run_data)


# # 
# # 
# # 
# # git log -1 `git rev-list -1 --date=local --before="2018-09-14T04:55:22" HEAD`
# # commit 4822bfeef6c5649b4f470906c44f8427ed9e4151
# # Author: Andrew Kiss <aek156@r358.(none)>
# # Date:   Fri Sep 14 04:55:09 2018 +1000
# # 
# #     2018-09-14 04:55:09: Run 92