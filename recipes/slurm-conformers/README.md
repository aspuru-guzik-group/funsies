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
floppy molecule, and verify that it can interconvert at room temperature.

### 2. Computational workflow
For our computational workflow, we will start from the SMILES representation
of a series of alkane diols, use
[OpenBabel](http://openbabel.org/wiki/Main_Page) to systematically find
conformers. Then, we will use Q-Chem to run expensive DFT calculation in order
to optimize conformer structures and compute their energies. Finally, we will
use Q-Chem's Freezing String method to find barriers between those
conformations.

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


