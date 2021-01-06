#!/usr/bin/bash
#SBATCH --cpus-per-task 20
#SBATCH --ntasks 8
#SBATCH --time=00:30:00
#SBATCH --job-name NAME

# environemnt loading etc
module load NiaEnv
module load conda3
source activate something

# number of workers
let "worker_num=(${SLURM_NTASKS})"

# redis server info
suffix='6379'
server_node=`hostname`:$suffix
export server_node

# run redis server (here we are *not* using a dedicated node, so memory could
# be an issue. If that's the case, use --exclude and srun.)
redis-server redis.conf &       # redis.conf should contain 'protected-mode no'
sleep 3

# run the funsies script
python3 SCRIPT_NAME.py ${server_node}
sleep 3

# now we run the workers
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}

# start workers in burst mode: work until no work is present, then quit
srun --ntasks=${worker_num} --cpus-per-task=${SLURM_CPUS_PER_TASK}\
     rq worker --url redis://${server_node}
