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

### 1. Problem statement
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

### 2. Computational workflow
For our computational workflow, we start from the SMILES representation of an
alkane diol. Then,
1. We use [OpenBabel](http://openbabel.org/wiki/Main_Page) to systematically
   find its conformers.
2. We optimize those conformers with [xtb](https://github.com/grimme-lab/xtb)
   and sort them with
   [CREST](https://xtb-docs.readthedocs.io/en/latest/crestcmd.html).
3. We use [Q-Chem](https://manual.q-chem.com/5.3/) to optimize the conformer
   structure using a DFT GGA functional and to compute conformer energies
   using a hybrid functional.
4. We then use Q-Chem's [Freezing String
   Method](https://manual.q-chem.com/5.3/sect_fstring.html) to estimate
   transition barriers between every pair of confomers. We locate transition
   states from the FSM simulations, and perform hybrid functional single point
   calculation for the transition states.
5. Finally, we export all the data into a big json file.

The entire workflow is in [workflow.py](./workflow.py). The workflow is fairly
complex; it was built to show off all the major strengths of funsies. It
contains dynamic workflow generation (to account for the fact that the number
of conformers is initially not known), shell commands and a variety of python
functions.

### 3. Deploy and execute
This workflow was executed on [Compute Canada's Niagara
cluster](https://docs.computecanada.ca/wiki/Niagara) using a conda environment
and local installations of Q-Chem and CREST. (See TODO for an example of
containerized deployement.)

To deploy, we first setup the conda environment using
```bash
conda env create -f environment.yml
```
This will create an environment with xtb, openbabel, funsies and their
dependencies. Then, we need to download and install CREST,
```bash
wget https://github.com/grimme-lab/crest/releases/download/v2.11/crest.tgz
tar -xzf crest.tgz $HOME/.local/bin
```
Now that all this is setup, we submit using [a standard SLURM submission
script](./slurm-submit.sh),
```bash
sbatch slurm-submit.sh
```
This script instantiate a redis server, starts eight workers on individual,
distributed nodes, runs the python workflow (and block until it finishes) then
shutdown workers and server.

That's it!

### 4. Data analysis
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

TODO: Do some error checking

TODO: Data analysis / graphs




