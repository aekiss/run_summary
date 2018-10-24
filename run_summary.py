#!/usr/bin/env python
"""

Tools to summarise access-om2 runs.
Should be run from an ACCESS-OM2 control directory.

Latest version: https://github.com/aekiss/run_summary
Author: Andrew Kiss https://github.com/aekiss
Apache 2.0 License http://www.apache.org/licenses/LICENSE-2.0.txt
"""
import os
import re
import glob
import subprocess
import datetime
from collections import OrderedDict
import csv
import copy

# on NCI the folllowing may require
# module use /g/data3/hh5/public/modules
# module load conda/analysis3
import yaml
import f90nml  # from https://f90nml.readthedocs.io/en/latest/


def get_sync_path(fname):
    """
    Return GDATADIR path from sync_output_to_gdata.sh.

    fname: sync_output_to_gdata.sh file path

    output: dict

    """
    with open(fname, 'r') as infile:
        for line in infile:
            #  NB: subsequent matches will replace earlier ones
            try:
                dir = line.split('GDATADIR=')[1].strip()
            except:
                continue
    return dir


def parse_pbs_log(fname):
    """
    Return dict of items from parsed PBS log file.

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


def parse_git_log(datestr):
    """
    Return dict of items from git log on given date.

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


def parse_mom_time_stamp(path, run):
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
    fname = path + '/output' + str(run).zfill(3) + '/ocean/time_stamp.out'
    parsed_items['Time stamp file'] = fname
    with open(fname, 'r') as infile:
        line = infile.readline()
        parsed_items['Model start time'] = datetime.datetime(
            *list(map(int, line.split()[0:-1]))).isoformat()
        line = infile.readline()
        parsed_items['Model end time'] = datetime.datetime(
            *list(map(int, line.split()[0:-1]))).isoformat()
    return parsed_items


def parse_config_yaml(path, run):
    """
    Returns dict of items from parsed config.yaml.

    run: run number

    output: dict
    """
    parsed_items = dict()
    fname = path + '/output' + str(run).zfill(3) + '/config.yaml'
    with open(fname, 'r') as infile:
        parsed_items = yaml.load(infile)
    return parsed_items


def parse_nml(path, run):
    """
    Returns dict of items from parsed namelists.

    run: run number

    output: dict
    """
    dir = path + '/output' + str(run).zfill(3) + '/'
    fnames = [dir + 'accessom2.nml'] + glob.glob(dir + '*/*.nml')
    parsed_items = dict()
    for fname in fnames:
        parsed_items[fname.split(dir)[1]] = f90nml.read(fname)
    return parsed_items


def git_diff(sha1, sha2):
    """
    Return dict of git-tracked differences between two commits.

    sha1, sha2: strings; sha1 should be earlier than or same as sha2
    """
    parsed_items = dict()
    p = subprocess.Popen('git diff --name-only ' + sha1 + ' ' + sha2,
                         stdout=subprocess.PIPE, shell=True)
    parsed_items['Changed files'] = p.communicate()[0].decode('ascii').split()
    p = subprocess.Popen('git log --pretty="%B\%x09" ' + sha1 + '..' + sha2,
                         stdout=subprocess.PIPE, shell=True)
    m = [s.strip('\n\\') for s in p.communicate()[0].decode('ascii').split('\t')][0:-1]
    m.reverse()  # put in chronological order
    parsed_items['Messages'] = m
    return parsed_items


def dictget(d, l):
    """
    Lookup item in nested dict using a list of keys.

    d: nested dict
    l: list of keys

    credit:
    https://stackoverflow.com/questions/14692690/access-nested-dictionary-items-via-a-list-of-keys
    """
    if len(l) == 1:
        return d[l[0]]
    return dictget(d[l[0]], l[1:])

print('Reading data', end = '')

# get jobname from config.yaml -- NB: we assume this is the same for all jobs
with open('config.yaml', 'r') as infile:
    jobname = yaml.load(infile)['jobname']

sync_path = get_sync_path('sync_output_to_gdata.sh')
p = subprocess.Popen('git rev-parse --abbrev-ref HEAD',
                     stdout=subprocess.PIPE, shell=True)
git_branch = p.communicate()[0].decode('ascii').strip()


# get data from all PBS job logs
run_data = dict()
for f in glob.glob('archive/pbs_logs/' + jobname + '.o*') + glob.glob(jobname + '.o*'):
    print('.', end = '')
    jobid = int(f.split('.o')[1])
    run_data[jobid] = dict()
    run_data[jobid]['PBS log'] = parse_pbs_log(f)
    run_data[jobid]['PBS log']['PBS log file'] = f
    # break

# get run data for all jobs
for jobid in run_data:
    print('.', end = '')
    pbs = run_data[jobid]['PBS log']
    date = pbs['Run completion date']
    if date is not None:
        run_data[jobid]['git log'] = parse_git_log(date)  # BUG: assumes the time zones match
        if pbs['Exit Status'] == 0:  # output dir belongs to this job only if Exit Status = 0
            try:
                run_data[jobid]['MOM_time_stamp.out'] = \
                    parse_mom_time_stamp(sync_path, pbs['Run number'])
            except:
                run_data[jobid]['MOM_time_stamp.out'] = \
                    parse_mom_time_stamp('archive', pbs['Run number'])
            try:
                run_data[jobid]['config.yaml'] = \
                    parse_config_yaml(sync_path, pbs['Run number'])
            except:
                run_data[jobid]['config.yaml'] = \
                    parse_config_yaml('archive', pbs['Run number'])
            try:
                run_data[jobid]['namelists'] = \
                    parse_nml(sync_path, pbs['Run number'])
            except:
                run_data[jobid]['namelists'] = \
                    parse_nml('archive', pbs['Run number'])
            # run_data[jobid]['accessom2.nml'] = \
            #     parse_accessom2_nml(pbs['Run number'])
            # TODO: save a list of files changed since last job (git diff --name-only SHA1 SHA2)
            # TODO: save a list of files changed since last successful run (git diff --name-only SHA1 SHA2)
            # TODO: save a list of commit hashes since last job
            # TODO: save a list of commit hashes since last successful run

all_run_data = copy.deepcopy(run_data)  # all_run_data includes failed jobs

# remove failed jobs from run_data
for jobid in all_run_data:
    print('.', end = '')
    pbs = all_run_data[jobid]['PBS log']
    date = pbs['Run completion date']
    if date is None:
        del run_data[jobid]
    elif pbs['Exit Status'] != 0:  # output dir belongs to this job only if Exit Status = 0
        del run_data[jobid]

# print(run_data)

# keys into run_data sorted by run number
sortedkeys = [k[0] for k in sorted([(k, v['PBS log']['Run number']) for (k, v) in run_data.items()], key=lambda t: t[1])]

# include changes in all commits since previous run
for i, jobnum in enumerate(sortedkeys):
    print('.', end = '')
    run_data[jobnum]['git diff'] = \
        git_diff(run_data[sortedkeys[max(i-1, 0)]]['git log']['Commit'],
                 run_data[jobnum]['git log']['Commit'])



# Specify the output format here.
# output_format is a list of (key, value) tuples, one for each column.
# keys are headers (must be unique)
# values are lists of keys into run_data (omitting job id)
#
# run_data dict structure:
# 
# run_data dict
#    L___ job ID dict
#           L___ 'PBS log' dict
#           L___ 'git log' dict (absent if PBS date is None)
#           L___ 'git diff' dict (absent if PBS date is None)
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
output_format = OrderedDict([
    ('Run number', ['PBS log', 'Run number']),
    ('Job Id', ['PBS log', 'Job Id']),
    ('Run start', ['MOM_time_stamp.out', 'Model start time']),
    ('Run end', ['MOM_time_stamp.out', 'Model end time']),
    ('Run length (years, months, seconds)', ['namelists', 'accessom2.nml', 'date_manager_nml', 'restart_period']),
    ('Run completion date', ['PBS log', 'Run completion date']),
    ('Queue', ['config.yaml', 'queue']),
    ('Service Units', ['PBS log', 'Service Units']),
    ('Walltime Used (s)', ['PBS log', 'Walltime Used']),
    ('NCPUs Used', ['PBS log', 'NCPUs Used']),
    ('Timestep (s)', ['namelists', 'accessom2.nml', 'accessom2_nml', 'ice_ocean_timestep']),
    ('ntdt', ['namelists', 'ice/cice_in.nml', 'setup_nml', 'ndtd']),
    ('distribution_type', ['namelists', 'ice/cice_in.nml', 'domain_nml', 'distribution_type']),
    ('ktherm', ['namelists', 'ice/cice_in.nml', 'thermo_nml', 'ktherm']),
    ('Git hash', ['git log', 'Commit']),
    ('Commit date', ['git log', 'Date']),
    ('Git-tracked file changes since previous run', ['git diff', 'Changed files']),
    ('Git log messages since previous run', ['git diff', 'Messages']),
    ])

# output csv file according to output_format
outfile = 'run_summary.csv'
print('\nWriting', outfile, end = '')
with open(outfile, 'w', newline='') as csvfile:
    csvw = csv.writer(csvfile, dialect='excel')
    csvw.writerow(['Summary report generated by run_summary.py'])
    csvw.writerow(['report generated:', datetime.datetime.now().isoformat()])
    csvw.writerow(['command directory path:', os.getcwd(), 'git branch:', git_branch])
    csvw.writerow(['hh5 output path:', sync_path])
    csvw.writerow(output_format.keys())  # header
    for jobnum in sortedkeys:
        print('.' end = '')
        csvw.writerow([dictget(run_data, [jobnum] + keylist) for keylist in output_format.values()])
print(' done.')


# TODO: run_diff : git diff between 2 runs
# TODO: job_diff : git diff between 2 jobs
