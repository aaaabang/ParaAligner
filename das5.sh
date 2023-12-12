#!/bin/bash

for file in $(find . -maxdepth 1 -type f \( -name "SlurmLog_*" -o -name ".nfs*" \)); do
    echo "remove temporary log: $file"
    rm "$file"
done

sbatch_output=$(sbatch job.slurm)
job_id=$(echo $sbatch_output | grep -oP '(?<=Submitted batch job )\d+')
log_file="SlurmLog_${job_id}.out"
echo "Check file '$log_file' to debuuuuuuuuuuuug! This file is dynamically generated, do not modify it during the process! hahaha"
