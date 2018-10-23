#!/usr/bin/env python
"""

Tools to summarise access-om2 runs.
Should be run from an ACCESS-OM2 control directory.

Latest version: https://github.com/aekiss/run_summary
Author: Andrew Kiss https://github.com/aekiss
Apache 2.0 License http://www.apache.org/licenses/LICENSE-2.0.txt
"""
import re
import glob
import subprocess
import datetime

# on NCI the folllowing may require
# module use /g/data3/hh5/public/modules
# module load conda/analysis3
import yaml
import f90nml  # from https://f90nml.readthedocs.io/en/latest/


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
        'git commit': getrun,  # NB: run with this number might have failed - check Exit Status
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
    """
    # possible BUG: what time zone flag should be use? local is problematic if run from overseas....?
    # use Popen for backwards-compatiblity with Python <2.7
    # pretty format is tab-delimited (%x09)
    p = subprocess.Popen('git log -1 --pretty="format:%H%x09%an%x09%aI%x09%B" '
                         + '`git rev-list -1 --date=local --before="'
                         + datestr + '" HEAD`',
                         stdout=subprocess.PIPE, shell=True)
    log = p.communicate()[0].decode('ascii').split('\t')
    # log = p.communicate()[0].decode('ascii').encode('ascii').split('\t')  # for python 2.6
    parsed_items = dict()
    parsed_items['Commit'] = log[0]
    parsed_items['Author'] = log[1]
    parsed_items['Date'] = log[2]
    parsed_items['Message'] = log[3].strip()
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


def parse_config_yaml(run):
    """
    Returns dict of items from parsed config.yaml.

    run: run number

    output: dict
    """
    parsed_items = dict()
    fname = 'archive/output' + str(run).zfill(3) + '/config.yaml'
    with open(fname, 'r') as infile:
        parsed_items = yaml.load(infile)
    return parsed_items


def parse_accessom2_nml(run):
    """
    Returns dict of items from parsed accessom2.nml.

    run: run number

    output: dict
    """
    parsed_items = dict()
    fname = 'archive/output' + str(run).zfill(3) + '/accessom2.nml'
    parsed_items = f90nml.read(fname)
    return parsed_items


def parse_nml(run):
    """
    Returns dict of items from parsed namelists.

    run: run number

    output: dict
    """
    dir = 'archive/output' + str(run).zfill(3) + '/'
    # print(dir)
    # fnames = glob.glob(dir + '*/*.nml').append(dir + 'accessom2.nml')
    fnames = [dir + 'accessom2.nml'] + glob.glob(dir + '*/*.nml')
    # print(fnames)
    # print(glob.glob(dir + '*/*.nml'))
    parsed_items = dict()
    for fname in fnames:
        parsed_items[fname.split(dir)[1]] = f90nml.read(fname)
    # print(parsed_items)
    return parsed_items


# get jobname from config.yaml -- NB: we assume this is the same for all jobs
with open('config.yaml', 'r') as infile:
    jobname = yaml.load(infile)['jobname']

# get data from all PBS job logs
run_data = dict()
for f in glob.glob('archive/pbs_logs/' + jobname + '.o*') + glob.glob(jobname + '.o*'):
    jobid = int(f.split('.o')[1])
    run_data[jobid] = dict()
    run_data[jobid]['PBS log'] = parse_pbs_log(f)
    run_data[jobid]['PBS log']['PBS log file'] = f

# get run data for all jobs
for jobid in run_data:
    pbs = run_data[jobid]['PBS log']
    date = pbs['Run completion date']
    if date is not None:
        run_data[jobid]['git log'] = parse_git_log(date)  # BUG: assumes the time zones match
        if pbs['Exit Status'] == 0:  # output dir belongs to this job only if Exit Status = 0
            run_data[jobid]['MOM_time_stamp.out'] = \
                parse_mom_time_stamp(pbs['Run number'])
            run_data[jobid]['config.yaml'] = \
                parse_config_yaml(pbs['Run number'])
            run_data[jobid]['namelists'] = \
                parse_nml(pbs['Run number'])
            # run_data[jobid]['accessom2.nml'] = \
            #     parse_accessom2_nml(pbs['Run number'])
            # TODO: save a list of files changed since last job (git diff --name-only SHA1 SHA2)
            # TODO: save a list of files changed since last successful run (git diff --name-only SHA1 SHA2)
            # TODO: save a list of commit hashes since last job
            # TODO: save a list of commit hashes since last successful run

print(run_data)

# run_data dict structure
# 
# run_data dict
#    L___ job ID dict
#           L___ 'PBS log' dict
#           L___ 'git log' dict (absent if PBS date is None)
#           L___ 'MOM_time_stamp.out' dict (absent if PBS exit status is not 0)
#           L___ 'config.yaml' dict (absent if PBS exit status is not 0)
#           L___ 'namelists' dict (absent if PBS exit status is not 0)
#                   L___ 'accessom2.nml' namelist
#                   L___ 'atmosphere/atm.nml' namelist
#                   L___ '/ice/cice_in.nml' namelist
#                   L___ 'ice/input_ice.nml' namelist
#                   L___ 'ice/input_ice_gfdl.nml' namelist
#                   L___ 'ice/input_ice_monin.nml' namelist
#                   L___ 'ocean/input.nml' namelist
#    L___ job ID dict
#           L___ ... etc

          

# TODO: run_diff : git diff between 2 runs
# TODO: job_diff : git diff between 2 jobs


# # 
# # 
# # 
# # git log -1 `git rev-list -1 --date=local --before="2018-09-14T04:55:22" HEAD`
# # commit 4822bfeef6c5649b4f470906c44f8427ed9e4151
# # Author: Andrew Kiss <aek156@r358.(none)>
# # Date:   Fri Sep 14 04:55:09 2018 +1000
# # 
# #     2018-09-14 04:55:09: Run 92