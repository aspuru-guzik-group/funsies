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
cluster](https://docs.computecanada.ca/wiki/Niagara) using the conda env
described above and local installations of Q-Chem and CREST. (See TODO for an
example of containerized deployement.)

To deploy, 




Then, we use Q-Chem to run expensive DFT calculation in order to optimize
conformer structures and compute their energies. Finally, we will use Q-Chem's
Freezing String method to find barriers between those conformations.

This is the kind of workflow where HPC resources are important, as it involves
difficult computations and expensive software. However, this won't be an issue
for you, the folks at home, because we have paired an already evaluated
funsies database to this problem! So go ahead, try running it on your laptop
or smartwatch or whatever!

To setup, we use a conda environment, installable from the `.yml` file in the
current directory,
```bash
conda env create -f environment.yml
```


