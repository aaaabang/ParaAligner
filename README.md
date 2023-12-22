# ParaAligner

ParaAligner is an implementation of Smith-Waterman algorithm in a distributed way.

## Usage

### Requirement

You need to connect to Das-5.

You can run `ssh dsys2319@fs0.das5.cs.vu.nl` to connect to our Das-5.

### Setup

Users on Das-5 can not use `sudo` command, so you need to run `pip3 install -r requirements.txt --user` to install dependencies. You may also
need to run `chmod +x ./ssw_test` to make SSW executable. The same approach applies to other bash scripts.

If `./ssw_test` could not be used, you can compile it yourself. Source code is [here](https://github.com/mengyao/Complete-Striped-Smith-Waterman-Library).

### Usage

Run `./experiment.sh` to perform experiment, all results could be seen in results.txt.

Run `./eval.sh` to verify the correctness of our implementation.

Run `run_local.py` to perform experiment on your own computer.

You can replace `main.py` with `main_ft.py` in scripts to test for fault tolerance.

