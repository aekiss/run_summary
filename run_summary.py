#!/usr/bin/env python
"""

Tools to summarise access-om2 runs.

Latest version: https://github.com/aekiss/run_summary
Author: Andrew Kiss https://github.com/aekiss
Apache 2.0 License http://www.apache.org/licenses/LICENSE-2.0.txt
"""
from __future__ import print_function
import sys
try:
    assert sys.version_info >= (3, 3)  # need python >= 3.3 for print flush keyword
except AssertionError:
    print('\nFatal error: Python version too old.')
    print('On NCI, do the following and try again:')
    print('   module use /g/data3/hh5/public/modules; module load conda/analysis3\n')
    raise

import os
import glob  # BUG: fails if payu module loaded - some sort of module clash with re
import subprocess
import datetime
from collections import OrderedDict
import csv
import copy

try:
    import yaml
    import f90nml  # from https://f90nml.readthedocs.io/en/latest/
except ImportError:  # BUG: don't get this exception if payu module loaded, even if on python 2.6.6
    print('\nFatal error: modules not available.')
    print('On NCI, do the following and try again:')
    print('   module use /g/data3/hh5/public/modules; module load conda/analysis3\n')
    raise
import nmltab  # from https://github.com/aekiss/nmltab

def get_sync_path(fname):
    """
    Return GDATADIR path from sync_output_to_gdata.sh.

    fname: sync_output_to_gdata.sh file path

    output: dict

    """
    with open(fname, 'r') as infile:
        for line in infile:
            # NB: subsequent matches will replace earlier ones
            try:
                dir = line.split('GDATADIR=')[1].strip().rstrip('/')
            except IndexError:  # 'GDATADIR=' not found - keep looking
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
        ns = s.strip('BKMGT')  # numerical part
        units = {'B': 1,
                 'KB': 2**10,
                 'MB': 2**20,
                 'GB': 2**30,
                 'TB': 2**40}
        return int(round(float(ns)*units[s[len(ns):]]))

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
                except IndexError:  # key not present in this line
                    continue

    # change to more self-explanatory keys
    rename_keys = {'git commit': 'Run number',
                   'Resource Usage on': 'Run completion date'}
    for oldkey, newkey in rename_keys.items():
        parsed_items[newkey] = parsed_items.pop(oldkey)

    return parsed_items


def parse_git_log(basepath, datestr):
    """
    Return dict of items from git log on given date.

    basepath: base directory path string

    datestr: date string

    output: dict
    """
    # possible BUG: what time zone flag should be use? local is problematic if run from overseas....?
    # use Popen for backwards-compatiblity with Python <2.7
    # pretty format is tab-delimited (%x09)
    p = subprocess.Popen('cd ' + basepath
                         + ' && git log -1 '
                         + '--pretty="format:%H%x09%an%x09%ai%x09%B" '
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


def parse_mom_time_stamp(paths):
    """
    Return dict of items from parsed MOM time_stamp.out.

    paths: list of base paths

    output: dict parsed from first matching time_stamp.out in paths

    example of MOM time_stamp.out content to parse:
        2001   9   1   0   0   0  Sep
        2001  11   1   0   0   0  Nov

    """
    parsed_items = dict()
    for path in paths:
        fname = os.path.join(path, 'ocean/time_stamp.out')
        if os.path.isfile(fname):
            parsed_items['Time stamp file'] = fname
            with open(fname, 'r') as infile:
                for key in ['Model start time', 'Model end time']:
                    line = infile.readline()
                    parsed_items[key] = datetime.datetime(
                        *list(map(int, line.split()[0:-1]))).isoformat()
            break
    return parsed_items


def parse_config_yaml(paths):
    """
    Return dict of items from parsed config.yaml.

    paths: list of base paths

    output: dict parsed from first matching config.yaml in paths
    """
    parsed_items = dict()
    for path in paths:
        fname = os.path.join(path, 'config.yaml')
        if os.path.isfile(fname):
            with open(fname, 'r') as infile:
                parsed_items = yaml.load(infile)
            break
    return parsed_items


def parse_nml(paths):
    """
    Return dict of items from parsed namelists.

    paths: list of base paths to parse for namelists

    output: dict
    """
    parsed_items = dict()
    parsed_items['accessom2.nml'] = None  # default value for non-YATM run
    for path in paths:
        fnames = [os.path.join(path, 'accessom2.nml')]\
                + glob.glob(os.path.join(path, '*/*.nml'))
        for fname in fnames:
            if os.path.isfile(fname):  # no accessom2.nml for non-YATM run
                parsed_items[fname.split(path)[1].strip('/')] \
                        = f90nml.read(fname)
    return parsed_items


def git_diff(basepath, sha1, sha2):
    """
    Return dict of git-tracked differences between two commits.

    basepath: base directory path string

    sha1, sha2: strings; sha1 should be earlier than or same as sha2
    """
    parsed_items = dict()
    p = subprocess.Popen('cd ' + basepath
                         + ' && git diff --name-only ' + sha1 + ' ' + sha2,
                         stdout=subprocess.PIPE, shell=True)
    parsed_items['Changed files'] = ', '.join(
        p.communicate()[0].decode('ascii').split())
    p = subprocess.Popen('cd ' + basepath
                         + ' && git log --ancestry-path --pretty="%B\%x09" '
                         + sha1 + '..' + sha2,
                         stdout=subprocess.PIPE, shell=True)
    m = [s.strip('\n\\') 
         for s in p.communicate()[0].decode('ascii').split('\t')][0:-1]
    m.reverse()  # put in chronological order
    if len(m) == 0:
        m = None
    parsed_items['Messages'] = m  # NB: will be None if there's no direct ancestry path from sha1 to sha2)
    return parsed_items


def dictget(d, l):
    """
    Lookup item in nested dict using a list of keys, or None if non-existent

    d: nested dict
    l: list of keys, or None
    """
    try:
        dl0 = d[l[0]]
    except (KeyError, TypeError):
        return None
    if len(l) == 1:
        return dl0
    return dictget(dl0, l[1:])


def run_summary(basepath=os.getcwd(), outfile=None):
    '''
    Generate run summary
    '''
    print('Reading run data ', end='')

    # get jobname from config.yaml -- NB: we assume this is the same for all jobs
    with open(os.path.join(basepath, 'config.yaml'), 'r') as infile:
        jobname = yaml.load(infile)['jobname']

    sync_path = get_sync_path(os.path.join(basepath, 'sync_output_to_gdata.sh'))
    if outfile is None:
        outfile = 'run_summary_' + os.path.split(sync_path)[1] + '.csv'

    p = subprocess.Popen('cd ' + basepath
                         + ' && git rev-parse --abbrev-ref HEAD',
                         stdout=subprocess.PIPE, shell=True)
    git_branch = p.communicate()[0].decode('ascii').strip()


    # get data from all PBS job logs
    run_data = dict()
    for f in glob.glob(os.path.join(basepath, 'archive/pbs_logs', jobname + '.o*'))\
           + glob.glob(os.path.join(basepath, jobname + '.o*'))\
           + glob.glob(os.path.join(sync_path, 'pbs_logs', jobname + '.o*')):
# NB: logs in archive may be duplicated in sync_path, in which case the latter is used
        print('.', end='', flush=True)
        jobid = int(f.split('.o')[1])
        run_data[jobid] = dict()
        run_data[jobid]['PBS log'] = parse_pbs_log(f)
        run_data[jobid]['PBS log']['PBS log file'] = f

    # get run data for all jobs
    for jobid in run_data:
        print('.', end='', flush=True)
        pbs = run_data[jobid]['PBS log']
        date = pbs['Run completion date']  # BUG: would be better to have time when run began, including time zone
        if date is not None:
            run_data[jobid]['git log'] = parse_git_log(basepath, date)
            # BUG: assumes no commits between run start and end
            # BUG: assumes the time zones match - no timezone specified in date - what does git assume? UTC?
            if pbs['Exit Status'] == 0:  # output dir belongs to this job only if Exit Status = 0
                outdir = 'output' + str(pbs['Run number']).zfill(3)
                paths = [os.path.join(sync_path, outdir),
                         os.path.join(basepath, 'archive', outdir)]
                run_data[jobid]['MOM_time_stamp.out'] = parse_mom_time_stamp(paths)
                run_data[jobid]['config.yaml'] = parse_config_yaml(paths)
                run_data[jobid]['namelists'] = parse_nml(paths)

    all_run_data = copy.deepcopy(run_data)  # all_run_data includes failed jobs

    # remove failed jobs from run_data
    for jobid in all_run_data:
        print('.', end='', flush=True)
        pbs = all_run_data[jobid]['PBS log']
        date = pbs['Run completion date']
        if date is None:
            del run_data[jobid]
        elif pbs['Exit Status'] != 0:  # output dir belongs to this job only if Exit Status = 0
            del run_data[jobid]

    # (jobid, run number) tuples sorted by run number - re-done below
    jobid_run_tuples = sorted([(k, v['PBS log']['Run number'])
                               for (k, v) in run_data.items()],
                              key=lambda t: t[1])
    if len(jobid_run_tuples) == 0:
        print('\nAborting: no successful jobs?')
        return

# Remove the older jobid if run number is duplicated - assume run was re-done
# (check by date rather than jobid, since jobid sometimes rolls over)
    prev_jobid_run = jobid_run_tuples[0]
    for jobid_run in jobid_run_tuples[1:]:
        if jobid_run[1] == prev_jobid_run[1]:  # duplicated run number
            if run_data[jobid_run[0]]['PBS log']['Run completion date']\
             > run_data[prev_jobid_run[0]]['PBS log']['Run completion date']:
                del run_data[prev_jobid_run[0]]
                prev_jobid_run = jobid_run
            else:
                del run_data[jobid_run[0]]
        else:
            prev_jobid_run = jobid_run

    # re-do (jobid, run number) tuples sorted by run number
    jobid_run_tuples = sorted([(k, v['PBS log']['Run number'])
                               for (k, v) in run_data.items()],
                              key=lambda t: t[1])
    if len(jobid_run_tuples) == 0:
        print('\nAborting: no successful jobs?')
        return

    # jobid keys into run_data sorted by run number
    sortedjobids = [k[0] for k in jobid_run_tuples]

    # make a 'timing' entry to contain model timestep and run length for both MATM and YATM runs
    # run length is [years, months, days, seconds] to accommodate both MATM and YATM
    for jobid in run_data:
        timing = dict()
        if run_data[jobid]['namelists']['accessom2.nml'] is None:  # non-YATM run
            timing['Timestep'] = run_data[jobid]['config.yaml']['submodels'][1]['timestep']  # MOM timestep
            rt = run_data[jobid]['config.yaml']['calendar']['runtime']
            timing['Run length'] = [rt['years'], rt['months'], rt['days'], 0]  # insert 0 seconds
        else:
            timing['Timestep'] = run_data[jobid]['namelists']['accessom2.nml']['accessom2_nml']['ice_ocean_timestep']
            rp = run_data[jobid]['namelists']['accessom2.nml']['date_manager_nml']['restart_period']
            timing['Run length'] = rp[0:2] + [0] + [rp[2]]  # insert 0 days
        run_data[jobid]['timing'] = timing

    # include changes in all git commits since previous run
    for i, jobid in enumerate(sortedjobids):
        print('.', end='', flush=True)
        run_data[jobid]['git diff'] = \
            git_diff(basepath,
                     run_data[sortedjobids[max(i-1, 0)]]['git log']['Commit'],
                     run_data[jobid]['git log']['Commit'])

    ###########################################################################
    # Specify the output format here.
    ###########################################################################
    # output_format is a list of (key, value) tuples, one for each column.
    # keys are headers (must be unique)
    # values are lists of keys into run_data (omitting job id)
    #
    # run_data dict structure:
    #
    # run_data dict
    #    L___ job ID dict
    #           L___ 'PBS log' dict
    #           L___ 'git log' dict
    #           L___ 'git diff' dict
    #           L___ 'MOM_time_stamp.out' dict
    #           L___ 'config.yaml' dict
    #           L___ 'timing' dict
    #           L___ 'namelists' dict
    #                   L___ 'accessom2.nml' namelist (or None if non-YATM run)
    #                   L___ 'atmosphere/atm.nml' namelist (only if YATM run)
    #                   L___ 'atmosphere/input_atm.nml' namelist (only if MATM run)
    #                   L___ '/ice/cice_in.nml' namelist
    #                   L___ 'ice/input_ice.nml' namelist
    #                   L___ 'ice/input_ice_gfdl.nml' namelist
    #                   L___ 'ice/input_ice_monin.nml' namelist
    #                   L___ 'ocean/input.nml' namelist
    #    L___ job ID dict
    #           L___ ... etc
    output_format = OrderedDict([
        ('Run number', ['PBS log', 'Run number']),
        ('Run start', ['MOM_time_stamp.out', 'Model start time']),
        ('Run end', ['MOM_time_stamp.out', 'Model end time']),
        ('Run length (years, months, days, seconds)', ['timing', 'Run length']),
        ('Job Id', ['PBS log', 'Job Id']),
        ('Run completion date', ['PBS log', 'Run completion date']),
        ('Queue', ['config.yaml', 'queue']),
        ('Service Units', ['PBS log', 'Service Units']),
        ('Walltime Used (s)', ['PBS log', 'Walltime Used']),
        ('NCPUs Used', ['PBS log', 'NCPUs Used']),
        ('MOM NCPUs', ['config.yaml', 'submodels', 1, 'ncpus']),
        ('CICE NCPUs', ['config.yaml', 'submodels', 2, 'ncpus']),
        ('Timestep (s)', ['timing', 'Timestep']),
        ('ntdt', ['namelists', 'ice/cice_in.nml', 'setup_nml', 'ndtd']),
        ('distribution_type', ['namelists', 'ice/cice_in.nml', 'domain_nml', 'distribution_type']),
        ('ktherm', ['namelists', 'ice/cice_in.nml', 'thermo_nml', 'ktherm']),
        ('Atmosphere executable', ['config.yaml', 'submodels', 0, 'exe']),
        ('MOM executable', ['config.yaml', 'submodels', 1, 'exe']),
        ('CICE executable', ['config.yaml', 'submodels', 2, 'exe']),
        ('CICE NCPUs', ['config.yaml', 'submodels', 2, 'ncpus']),
        ('Git hash', ['git log', 'Commit']),
        ('Commit date', ['git log', 'Date']),
        ('Git-tracked file changes since previous run', ['git diff', 'Changed files']),
        ('Git log messages since previous run', ['git diff', 'Messages']),
        ])
    ###########################################################################

    if True:  # whether to output all namelist changes
        output_format_nmls = OrderedDict()
        nmls_any_runs = set(run_data[list(run_data.keys())[0]]['namelists'].keys())
        nmls_all_runs = nmls_any_runs
        # avoid dict comprehension here to avoid python<2.7 syntax error
        nmls_no_runs = dict([(k, True) for k in nmls_any_runs])  # True for namelists that are None for all runs
        # nmls_no_runs = {k: True for k in nmls_any_runs}  # True for namelists that are None for all runs
        for jobid in run_data:
            run_nmls = run_data[jobid]['namelists']
            nmls_any_runs = set(run_nmls.keys()) | nmls_any_runs
            nmls_all_runs = set(run_nmls.keys()) & nmls_all_runs
            for nml in set(nmls_all_runs):
                if run_nmls[nml] is None:
                    nmls_all_runs.remove(nml)
            for nml in run_nmls:
                newnone = (nml is None)
                if nml in nmls_no_runs:
                    nmls_no_runs[nml] = nmls_no_runs[nml] and newnone
                else:
                    nmls_no_runs.update({nml: newnone})
        for nml in set(nmls_any_runs):
            if nmls_no_runs[nml]:
                nmls_any_runs.remove(nml)

        # add every changed group/variable in nml files that exist in all runs
        for nml in nmls_all_runs:
            # avoid dict comprehension here to avoid python<2.7 syntax error
            nmllistall = dict([(jobid,
                              copy.deepcopy(run_data[jobid]['namelists'][nml]))
                              for jobid in run_data])
            # nmllistall = {jobid: copy.deepcopy(run_data[jobid]['namelists'][nml])
            #               for jobid in run_data}
            groups = nmltab.superset(nmltab.nmldiff(nmllistall))
            for group in groups:
                for var in groups[group]:
                    ngv = [nml, group, var]
                    output_format_nmls.update(OrderedDict([
                        (' -> '.join(ngv), ['namelists'] + ngv)]))

        # add all group/variables in nml files that exist in only some runs
        for nml in nmls_any_runs - nmls_all_runs:
            nmllistall = dict()
            for jobid in run_data:
                if nml in run_data[jobid]['namelists']:
                    if run_data[jobid]['namelists'][nml] is not None:
                        nmllistall.update({jobid:
                              copy.deepcopy(run_data[jobid]['namelists'][nml])})
            groups = nmltab.superset(nmllistall)
            for group in groups:
                for var in groups[group]:
                    ngv = [nml, group, var]
                    output_format_nmls.update(OrderedDict([
                        (' -> '.join(ngv), ['namelists'] + ngv)]))

        # alphabetize
        output_format_nmls = OrderedDict([(k, output_format_nmls[k])
                                 for k in sorted(output_format_nmls.keys())])

        # add output_format entries for every namelist variable that has changed in any run
        output_format.update(output_format_nmls)

    # output csv file according to output_format above
    print('\nWriting', outfile)
    with open(outfile, 'w', newline='') as csvfile:
        csvw = csv.writer(csvfile, dialect='excel')
        csvw.writerow(['Summary report generated by run_summary.py, https://github.com/aekiss/run_summary'])
        csvw.writerow(['report generated:', datetime.datetime.now().isoformat()])
        csvw.writerow(['command directory path:', basepath, 'git branch:', git_branch])
        csvw.writerow(['hh5 output path:', sync_path])
        csvw.writerow(output_format.keys())  # header
        for jobid in sortedjobids:
            csvw.writerow([dictget(run_data, [jobid] + keylist) for keylist in output_format.values()])
    print('Done.')
    return


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=
        'Summarise ACCESS-OM2 runs.\
        Latest version and help: https://github.com/aekiss/run_summary')
    parser.add_argument('-o', '--outfile', type=str,
                        metavar='file',
                        default=None,
                        help="output file path; default is 'run_summary_<dir name on hh5>.csv';\
                        WARNING: will be overwritten")
    parser.add_argument('path', metavar='path', type=str, nargs='?',
                        help='ACCESS-OM2 control directory path; default is current working directory')
    args = parser.parse_args()
    outfile = vars(args)['outfile']
    basepath = vars(args)['path']
    if outfile is None:
        if basepath is None:
            run_summary()
        else:
            run_summary(basepath=basepath)
    else:
        if basepath is None:
            run_summary(outfile=outfile)
        else:
            run_summary(basepath=basepath, outfile=outfile)

# TODO: run_diff : git diff between 2 runs