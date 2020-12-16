"""A quantum chemistry example."""
# external
import redis
from rq import Queue

# module
from funsies import run, task, transformer

# To run this example, you will need openbabel and xtb installed and on path
# on all worker nodes.

# as usual, we start with the Redis server and setting up the job queue.
db = redis.Redis()
queue = Queue(connection=db)

# Set some good defaults for the jobs on queue. In general, it's not worth
# setting ttl or result_ttl to other values because when doing chemistry
# simulations, we are not going to run 1 million jobs per minute and thus
# crash because of out-of-memory errors. However, as all the jobs are really
# long, we don't want jobs to be cancelled just because it's taking a while to
# find a worker.
job_defaults = dict(
    timeout="3h",  # how long each job has
    ttl="10d",  # how long jobs are kept on queue
    result_ttl="10d",  # how long job result objects are kept
)

# our task is to take a bunch of molecules, given as SMILES, make conformers
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
def get_gap(inp: bytes) -> bytes:
    """Take HOMO-LUMO gap in inp, and output it to out."""
    for line in inp.decode().splitlines():
        # we are looking for this line
        # | HOMO-LUMO GAP               1.390012170128 eV   |
        if "HOMO-LUMO GAP" in line:
            f = float(line.strip()[18:-7].strip())
            return str(f).encode()
    return b""


# We start by running obabel to transform those from SMILES to 3d conformers.
for i, smi in enumerate(smiles):
    inf = f"{i}.smi"
    t1 = task(
        db,
        f"obabel {inf} --addh --gen3d -O init.xyz",
        inp={inf: smi},
        out=["init.xyz"],
    )

    job1 = queue.enqueue_call(run, [t1], **job_defaults)  # this starts running the job

    # t1 outputs can already be used as inputs in other jobs, even if the task
    # is not yet completed, because they are not actual values. They are
    # instead pointers to file on the redis server.

    # we use the output init.xyz from t1 as the input to xtb.
    t2 = task(
        db,
        "xtb init.xyz --opt --parallel 1",
        inp={"init.xyz": t1.outputs["init.xyz"]},
        out=["xtbopt.xyz"],
    )

    # note that we have a dependency on t1 and we need to tell the queue about
    # it.
    job2 = queue.enqueue_call(run, [t2], depends_on=job1, **job_defaults)

    # Now the HOMO-LUMO gap is in the xtb std output. To get it, we will use a
    # python IO transform.
    xtb_out = t2.commands[0].stdout

    # if instead of a commandline command we pass a python function fun to
    # task(), we create a transformer that takes input files in inp to output
    # files in out using the function fun(*inputs, *outputs), where inputs and
    # outputs are IO sources and sinks.
    tr = transformer(db, get_gap, inp=[xtb_out])
    queue.enqueue_call(run, [tr], depends_on=job2, **job_defaults)
