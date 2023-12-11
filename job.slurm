#!/bin/bash
#SBATCH --job-name=launch_parallel_SW
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=8
#SBATCH --time=00:05:00
#SBATCH --output=SlurmLog_%j.out

srun python3 -u main.py
