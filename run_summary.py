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

def parse_pbs_log(fname):
    """
    Returns dict of items from parsed pbs log.

    fname: PBS log file name or path

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
        return l[0]

    def getrun(l):
        return int(l[4].rstrip('"'))

    def getjob(l):
        return int(l[0].split('.')[0])

    def getint(l):
        return int(l[0])

    def getfloat(l):
        return float(l[0])

    def getsec(l):  # convert hh:mm:ss to sec
        return sum(x * int(t) for x, t in zip([3600, 60, 1], l[0].split(':')))

    def getdatetime(l):
        return l[0]+'T'+l[1].rstrip(':')

    search_items = {  # keys are strings to search for; items are functions to apply to list of strings following key
        'git commit': getrun,
        'Resource Usage on': getdatetime,
        'Job Id:': getjob,
        'Project': null,
        'Exit Status:': getint,
        'Service Units:': getfloat,
        'NCPUs Requested:': getint,
        'NCPUs Used:': getint,
        'CPU Time Used:': getsec,
        'Memory Requested:': null,  # TODO: convert Gb, Tb etc to bytes?
        'Memory Used:': null,  # TODO: convert Gb, Tb etc to bytes?
        'Walltime requested:': getsec,
        'Walltime Used:': getsec,
        'JobFS requested:': null,  # TODO: convert Gb, Tb etc to bytes?
        'JobFS used:': null}  # TODO: convert Gb, Tb etc to bytes?
    parsed_items = search_items.fromkeys(search_items, None)

    with open(fname, 'r') as infile:
        for line in infile:
            for key, op in search_items.items():
                try:
                    parsed_items[key] = op(line.split(key)[1].split())
                except:
                    continue
    return parsed_items

parse_pbs_log('test/01deg_jra55_iaf.o1399186')

