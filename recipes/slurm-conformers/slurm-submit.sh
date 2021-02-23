#!/bin/bash
#SBATCH -A rrg-aspuru
#SBATCH --nodes 8
#SBATCH --cpus-per-task 40
#SBATCH --ntasks 8
#SBATCH --time=2:00:00
#SBATCH --job-name funsies-conformers

# python
module load conda3
source activate slurm-conformers

# QChem
export TMPDIR=$SLURM_TMPDIR
export QC=/scinet/niagara/software/commercial/qc53
export QCAUX=$QC/qcaux
source $QC/qcenv.sh

# Path to CREST
export PATH=$HOME/.local/bin:$PATH

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

# Cleanup any rq temporary things 
funsies clean

# Save result and quit
redis-cli --rdb results.rdb
redis-cli shutdown 

