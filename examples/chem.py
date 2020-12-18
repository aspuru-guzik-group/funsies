"""A quantum chemistry example."""
# std
import sys

# external
import redis
from rq import Queue

# module
from funsies import runall, task, transformer, pull_file

# To run this example, you will need openbabel and xtb installed and on path
# on all worker nodes.

# Setup the Redis server
# ----------------------
# this will ensure that the database is fully persistent
# between runs.
db = redis.Redis()
db.config_set("appendonly", "yes")  # type:ignore


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
for i, smi in enumerate(smiles):
    t1 = task(
        db,
        f"obabel in.smi --addh --gen3d -O init.xyz",
        inp={"in.smi": smi},
        out=["init.xyz"],
    )
    # t1 outputs can already be used as inputs in other jobs, even if the task
    # is not yet completed, because they are not actual values: they are
    # instead pointers to file on the redis server.

    # we use the output init.xyz from t1 as the input to xtb.
    t2 = task(
        db,
        "xtb init.xyz --opt --parallel 1",
        inp={"init.xyz": t1.out["init.xyz"]},
        out=["xtbopt.xyz"],
    )

    # Now the HOMO-LUMO gap is in the xtb std output. To get it, we will use a
    # python IO transform on the xtb stdout,
    xtb_out = t2.commands[0].stdout

    # We use get_gap defined above to do the transformation. We also add the
    # SMILES string so that we can keep track of molecules.
    tr = transformer(db, get_gap, inp=[t1.inp["in.smi"], xtb_out])
    outputs += [tr.out[0]]


# Final transformer that joins all the outputs.
def join(*args: bytes) -> bytes:
    out = b""
    for a in args:
        out += a
    return out


tr = transformer(db, join, inp=outputs)

if sys.argv[-1] != "read":
    # Setup the RQ job queue and run
    # ------------------------------
    # Set some good defaults for the jobs on queue. In general, it's not worth
    # setting ttl or result_ttl to other values because when doing chemistry
    # simulations, we are not going to run 1 million jobs per minute and thus
    # crash because of out-of-memory errors. However, as all the jobs are really
    # long, we don't want jobs to be cancelled just because it's taking a while to
    # find a worker.
    queue = Queue(connection=db)
    job_defaults = dict(
        timeout="3h",  # how long each job has
        ttl="10d",  # how long jobs are kept on queue
        result_ttl="10d",  # how long job result objects are kept
    )
    # run everything by using the fact that the last transformer depends on all
    # the outputs
    runall(queue, tr.task_id)

else:
    # Analyze / compile results
    # -------------------------
    # Traditionally, we would then use a wait() routine to wait
    # for all results to come in and then do final DB join/concat operations.
    # Here, we use the fact that all simulations were recorded to simply
    # re-run everything in sync mode (meaning without separate worker
    # threads), playing back all the operations using the memoized data.
    queue = Queue(connection=db, is_async=False)

    # tip: set the read_only flag to ensure you are not doing any operations
    # that would crash your weak local data analysis machine!
    runall(queue, tr.task_id, no_exec=True)

    # Because it's not async, we know that the outputs of the transformer are
    # fully populated here already, so we can just print them.
    out = pull_file(db, tr.out[0])

    # Voila! Results to stdout using simply python chem.py read
    print(out.decode().rstrip() + "\n")
