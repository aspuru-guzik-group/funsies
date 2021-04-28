## funsies + SLURM = 💖
A major reason why funsies was created is to orchestrate computationally
expensive complicated workflows at high-performance computing centers. In
effect, funsies tries to fill the niche between full-blown workflow engine
like [airflow](https://airflow.apache.org/) and hastily stitched together
pieces of bash script, [GNU parallel](https://www.gnu.org/software/parallel/)
and FORTRAN 77.

In this recipe, I describe how to integrate funsies with
[slurm](https://slurm.schedmd.com/documentation.html) the de facto standard
resource manager for scientific computing clusters.

### Problem statement
Molecules are usually represented as simple stick diagrams. Obviously, they
don't move while on paper, which gives the impression that they are static.
That's not the case in real life.

Molecules are dynamic entities and they freely flop around in vacuum. The
stable geometries of a molecule (its potential energy minima) that can rapidly
interconvert at room temperature are called conformers. The floppier the
molecule, the more conformers it has. 

In this problem, we will use funsies to compute conformer energies for a
floppy molecule, and verify that the conformers can interconvert at room
temperature.

### Computational workflow
For our computational workflow, we start from the SMILES representation of an
alkane diol. Then,
1. We use [OpenBabel](http://openbabel.org/wiki/Main_Page) to systematically
   find its conformers.
2. We optimize each of those conformers with
   [xtb](https://github.com/grimme-lab/xtb) and sort them.
3. We output the result as a JSON file.

The entire workflow is in [workflow.py](./workflow.py). This workflow contains
dynamic workflow generation (to account for the fact that the number of
conformers is initially not known), shell commands and some python functions.

### Deploy and execute
To deploy, we first setup a conda environment,
```bash
conda create -n funsies
conda install -c conda-forge xtb openbabel
conda install redis-server
pip install funsies
```
This will create an environment with xtb, openbabel, funsies and their
dependencies. Now that all this is setup, we submit using [a standard SLURM submission
script](./slurm-submit.sh),
```bash
sbatch slurm-submit.sh
```
The header of the submission script should be modified to match that of your
SLURM account.

What does the submission script do? It instantiate a redis server, starts a
number of workers on individual compute nodes, tells worker processes how to
find the server, runs the python workflow then shutdown workers and server.

All this in only 9 lines of shell!

The final step of the job dumps an image of the redis database in
`results.rdb`. This image includes all temporary results. To run a "mock"
version of the computation, simply rename or copy the file to `dump.rdb` and
start a server. For example, doing
```bash
cp results.rdb dump.rdb
redis-server &
python workflow.py
```
will run through the workflow again locally, but without recomputing anything.
To look at the computational graph using graphviz one can simply do,
```bash
cp results.rdb dump.rdb
redis-server &
funsies graph > graph.dot
dot -Tpdf graph.dot > graph.pdf
```
This will generate [the graph shown here.](./graph.pdf) 
