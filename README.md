# run_summary

Creates a csv file summarising [ACCESS-OM2](https://github.com/OceansAus/access-om2) runs.

## Usage: 
```
run_summary.py [-h] [-o file] [path]

positional arguments:
  path                  ACCESS-OM2 control directory path; default is current
                        working directory

optional arguments:
  -h, --help            show this help message and exit
  -o file, --outfile file
                        output file path; default is 'run_summary_<dir name on hh5>.csv'; 
                        WARNING: will be overwritten
```                        
                        
- Requires Python 3, [nmltab](https://github.com/aekiss/nmltab) and a reasonably recent version of git
- On NCI you may need to do something like this:
```
module load git  # NCI's git is newer than in /projects/v45/modules
module use /g/data3/hh5/public/modules
module load conda/analysis3
module list
git --version  # version >= 2.9.5 seems to work; use 'module unload git/x.x.x' to unload older version(s) x.x.x
```
- The data to be output is easily customised by editing `output_format` in `run_summary.py`.

