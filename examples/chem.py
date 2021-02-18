"""A quantum chemistry example."""
# funsies imports
from funsies import execute, Fun, reduce, shell
from funsies.utils import concat, stop_if


# To run this example, you will need openbabel and xtb installed and on path
# on all worker nodes.


# This is the routine that outputs the HOMO-LUMO gap out of the xtb output.
def get_gap(smi: bytes, xtbout: bytes) -> bytes:
    """Take HOMO-LUMO gap in inp, and output it to out."""
    for line in xtbout.decode().splitlines():
        # we are looking for this line
        # | HOMO-LUMO GAP               1.390012170128 eV   |
        if "HOMO-LUMO GAP" in line:
            gap = float(line.strip()[18:-7].strip())
            break
    else:
        raise RuntimeError("HOMO-LUMO gap was not found!")

    # output is a csv row
    output = f"{smi.decode()},{gap}\n"
    return output.encode()


# By default, the Fun context manager connects to a Redis instance on
# localhost, but a different redis instance can be passed to it.
with Fun():
    # Start of computational workflow
    # -------------------------------
    # Our task is to take a bunch of molecules, given as SMILES, make conformers
    # for them, optimize those conformers with xtb and extract their xtb HOMO-LUMO
    # gap.

    # Below is a list of random small drug molecules,
    smiles = [
        r"CC\C=C/C\C=C/C\C=C/CCCCCCCC(O)=O",
        r"CCN(CC)CC1=C(O)C=CC(NC2=C3C=CC(Cl)=CC3=NC=C2)=C1",
        # next smiles is invalid:
        r"CCC(C3)C1(CC)C(=O)NC(=O)NC1=O",
        r"CN(C)CCCC1(OCC2=C1C=CC(=C2)C#N)C1=CC=C(F)C=C1",
        r"NC12CC3CC(CC(C3)C1)C2",
    ]

    # We start by running obabel to transform those from SMILES to 3d conformers.
    outputs = []
    for smi in smiles:
        make_3d = shell(
            "obabel initial.smi --addh --gen3d --ff uff --minimize -O conf0.xyz",
            inp={"initial.smi": smi},
            out=["conf0.xyz"],
        )
        # t1 outputs can already be used as inputs in other jobs, even if the task
        # is not yet completed, because they are not actual values: they are
        # instead pointers to file on the redis server.

        # first we make sure that output is not empty, meaning that obabel crashed
        geom = stop_if(lambda x: len(x.strip()) == 0, make_3d.out["conf0.xyz"])

        # we use the output init.xyz from t1 as the input to xtb.
        optimize = shell(
            "xtb init.xyz --opt --parallel 1",
            inp={"init.xyz": geom},
            out=["xtbopt.xyz", ".xtboptok"],
            env={"OMP_NUM_THREADS": "1"},
        )

        # Now the HOMO-LUMO gap is in the xtb std output. To get it, we will
        # use a python IO transform on the xtb stdout. We use get_gap defined
        # above to do the transformation. We also add the SMILES string so
        # that we can keep track of molecules.
        outputs += [reduce(get_gap, smi, optimize.stdout)]

    # Final reduction
    tr = concat(*outputs, join="", strict=False)

    # execute
    execute(tr)

    # print out the hash of interest
    print(f"results can be found at {tr.hash}")
