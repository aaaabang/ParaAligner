#!/bin/bash

rm ./output/*
for file in $(find . -maxdepth 1 -type f \( -name "SlurmLog_*" -o -name ".nfs*" \)); do
    # echo "remove temporary log: $file"
    rm "$file"
done

sbatch_output=$(sbatch job.slurm)
job_id=$(echo $sbatch_output | grep -oP '(?<=Submitted batch job )\d+')

while [ ! -f ./output/match.txt ]; do
    sleep 1
done

sleep 5

max_number=$(awk '{print $1}' ./output/match.txt | sort -nr | head -n 1)

# you can change arguments by checking `alg.py`
output=$(./ssw_test -c -m 3 -x 3 -o 2 -e 2 data/fna_db/covid_S.fna data/patterns/test.fna | grep 'optimal_alignment_score')
alignment_score=$(echo "$output" | grep -oP '(?<=optimal_alignment_score: )\d+' | head -n 1)

echo "Max number from match.txt: $max_number"
echo "Alignment score from ssw_test: $alignment_score"

if [ "$alignment_score" -eq "$max_number" ]; then
    echo "The optimal alignment score ($alignment_score) is equal to the maximum number ($max_number) in match.txt."
else
    echo "The optimal alignment score ($alignment_score) is not equal to the maximum number ($max_number) in match.txt."
fi
