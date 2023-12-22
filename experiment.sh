#!/bin/bash

# 设置节点和任务数的范围
start_nodes=1
end_nodes=12
start_ntasks_per_node=1
end_ntasks_per_node=12

# 循环遍历节点和任务数的范围
for ((nodes=end_nodes; nodes>=start_nodes; nodes--)); do
    for ((ntasks_per_node=end_ntasks_per_node; ntasks_per_node>=start_ntasks_per_node; ntasks_per_node--)); do

        # 使用 sed 修改 job.slurm 文件
        sed -i "s/--nodes=[0-9]*/--nodes=$nodes/" job.slurm
        sed -i "s/--ntasks-per-node=[0-9]*/--ntasks-per-node=$ntasks_per_node/" job.slurm

        # 执行原本的脚本内容
        rm ./output/*
        for file in $(find . -maxdepth 1 -type f \( -name "SlurmLog_*" -o -name ".nfs*" \)); do
            rm "$file"
        done

        sbatch_output=$(sbatch job.slurm)
        job_id=$(echo $sbatch_output | grep -oP '(?<=Submitted batch job )\d+')

        start_time=$(date +%s)

        initial_count=$(ls -1 ./output | wc -l)
        current_count=$initial_count

        while [ $current_count -le $initial_count ]; do
            sleep 1
            current_count=$(ls -1 ./output | wc -l)
        done

        end_time=$(date +%s)
        duration=$((end_time - start_time))

        echo "$nodes * $ntasks_per_node ($job_id):" >> results.txt
        echo "TIME: $duration s" >> results.txt
        scancel $job_id
        sleep 10

        total_memory=$(sacct -j $job_id --format=MaxRSS | awk '
            NR > 2 { 
                gsub(/[KMG]/, "", $1); 
                sum += $1; 
            } 
            END { 
                print sum "K"; 
            }')
        echo "MEM: $total_memory" >> results.txt
        echo -e "\n" >> results.txt
    done
done
