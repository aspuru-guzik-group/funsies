#!/bin/bash
#SBATCH -A ACCOUNT
#SBATCH --nodes NUM_NODES
#SBATCH --cpus-per-task NUM_TASKS
#SBATCH --ntasks 8
#SBATCH --time=2:00:00
#SBATCH --job-name funsies-on-slurm
# fill ^^ with your usual slurm parameters

# if you are running on a HPC cluster, you are probably using conda? in which
# case you would do something like...
module load conda3
source activate funsies
# to run the script you would specifically need an env with openbabel and xtb
# in it
# conda install -c conda-forge xtb openbabel

# load more software etc....

# ----------------------------------------------------------------------------
# the part below is the important one!

# get address of the nodes that holds the server
server_node=`hostname`
export server_node

# Initialize redis server
redis-server redis.conf &

# Wait for server to be done loading
funsies wait

# Start worker processes and connect to the redis server
srun --ntasks=${SLURM_NTASKS} --cpus-per-task=${SLURM_CPUS_PER_TASK} \
	-o worker_%j.%t.out -e worker_%j.%t.err \
	funsies --url redis://${server_node} worker &

# Run job script
python3 workflow.py
# (script blocks until final result is computed)

# Shutdown workers gracefully
funsies shutdown

# Save result and quit
redis-cli --rdb results.rdb
redis-cli shutdown 

