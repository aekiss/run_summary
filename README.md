# run_summary

Creates an Excel-compatible .csv file summarising [ACCESS-OM2](https://github.com/OceansAus/access-om2) experiments, tabulating each run number and its dates, PBS job id, walltime, service units, timestep, file changes, git hashes, git commit messages, all namelist changes, and more. You can also easily customise what is output.

## Usage

The simplest way to run it is to put `run_summary.py` and `nmltab.py` (from [here](https://github.com/aekiss/nmltab)) in the run control directory you want to summarise, then type `./run_summary.py`. After some processing (which might take a few minutes) it will generate a .csv file summarising your runs which you can open in Excel or similar.

You can also put `run_summary.py` and `nmltab.py` anywhere in your search path and then do `run_summary.py my/control/dir/path` to specify which ACCESS-OM2 control directory to summarise. You can also use wildcards to summarise multiple ACCESS-OM2 control directories.

Usage details:
```
run_summary.py [-h] [-o file] [path [path ...]]

positional arguments:
  path                  zero or more ACCESS-OM2 control directory paths;
                        default is current working directory

optional arguments:
  -h, --help            show this help message and exit
  -o file, --outfile file
                        output file path; default is 'run_summary_<dir name on
                        hh5>.csv'; WARNING: will be overwritten
```

### Customising the output
To customise what is output, simply edit `output_format` in `run_summary.py`.

`run_summary.py` collects much more data than it outputs by default to the .csv file so there are plenty of extra things to add if you want them.  Changes to any variable in any .nml file will automatically be output. 

## Requirements
- Requires Python 3, [nmltab](https://github.com/aekiss/nmltab) and a reasonably recent version of git.
- On NCI you may need to do something like this:
```
module load git
module use /g/data3/hh5/public/modules
module load conda/analysis3
```
