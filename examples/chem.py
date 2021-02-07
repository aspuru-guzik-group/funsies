"""A quantum chemistry example."""
# module
from funsies import execute, Fun, options, reduce, shell, take, wait_for
from funsies.utils import concat

# To run this example, you will need openbabel and xtb installed and on path
# on all worker nodes.

# By default, the Fun context manager connects to a Redis instance on
# localhost, but a different redis instance can be passed to it.
with Fun(defaults=options(timeout=60.0)):
    # Start of computational workflow
    # -------------------------------
    # Our task is to take a bunch of molecules, given as SMILES, make conformers
    # for them, optimize those conformers with xtb and extract their xtb HOMO-LUMO
    # gap.

    # Below is a list of random small drug molecules,
    smiles = [
        r"CC\C=C/C\C=C/C\C=C/CCCCCCCC(O)=O",
        r"CCN(CC)CC1=C(O)C=CC(NC2=C3C=CC(Cl)=CC3=NC=C2)=C1",
        r"NCCCNCCSP(O)(O)=O",
        r"NC12CC3CC(CC(C3)C1)C2",
        r"CC(C)NCC(O)COC1=CC=C(CCOCC2CC2)C=C1",
        r"CCC(C)C1(CC)C(=O)NC(=O)NC1=O",
        r"CCN1N=C(C(O)=O)C(=O)C2=CC3=C(OCO3)C=C12",
        r"CN(C)CCCC1(OCC2=C1C=CC(=C2)C#N)C1=CC=C(F)C=C1",
        r"NC1=CC=NC=C1",
        r"NC(=N)N1CCC2=CC=CC=C2C1",
        r"CCN(CC)CCOC(=O)C1(CCCCC1)C1CCCCC1",
        r"FC(F)OC(F)(F)C(F)Cl",
        r"CCNC1C2CCC(C2)C1C1=CC=CC=C1",
        r"FC1=CNC(=O)NC1=O",
    ]

    # # This is the routine that outputs the HOMO-LUMO gap out of the xtb output.
    def get_gap(smi: bytes, xtbout: bytes) -> bytes:
        """Take HOMO-LUMO gap in inp, and output it to out."""
        for line in xtbout.decode().splitlines():
            # we are looking for this line
            # | HOMO-LUMO GAP               1.390012170128 eV   |
            if "HOMO-LUMO GAP" in line:
                gap = float(line.strip()[18:-7].strip())
                break

        # output is a csv row
        output = f"{smi.decode()},{gap}\n"
        return output.encode()

    # We start by running obabel to transform those from SMILES to 3d conformers.
    outputs = []
    for _, smi in enumerate(smiles):
        t1 = shell(
            "obabel in.smi --addh --gen3d -O init.xyz",
            inp={"in.smi": smi},
            out=["init.xyz"],
        )
        # t1 outputs can already be used as inputs in other jobs, even if the task
        # is not yet completed, because they are not actual values: they are
        # instead pointers to file on the redis server.

        # we use the output init.xyz from t1 as the input to xtb.
        t2 = shell(
            "xtb init.xyz --opt --parallel 1",
            inp={"init.xyz": t1.out["init.xyz"]},
            out=["xtbopt.xyz"],
        )

        # Now the HOMO-LUMO gap is in the xtb std output. To get it, we will use a
        # python IO transform on the xtb stdout,
        xtb_out = t2.stdout

        # We use get_gap defined above to do the transformation. We also add the
        # SMILES string so that we can keep track of molecules.
        outputs += [reduce(get_gap, t1.inp["in.smi"], xtb_out)]

    # Final transformer that joins all the outputs.
    tr = concat(*outputs, join="")

    # Execute and wait for results
    execute(tr)
    wait_for(tr)
    out = take(tr)
    print(out.decode().rstrip() + "\n")
