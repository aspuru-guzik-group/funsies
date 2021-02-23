"""Example workflow to generate conformers and conformation changes."""
from __future__ import annotations

import json

import funsies as f
from funsies.types import Artefact
from funsies.utils import not_empty


# --------------------------------------------------------------------------------- #
# QChem input files

# "low" theory level
THEORY = """
method  revPBE
DFT_D D3_BJ
basis def2-SVP
aux_basis rijk-def2-svp
ri_j true
ri_k true
symmetry false
sym_ignore true
"""
# "high" theory level
THEORY2 = """
method  revPBE0
DFT_D D3_BJ
basis def2-TZVP
aux_basis rijk-def2-tzvp
ri_j true
ri_k true
symmetry false
sym_ignore true
"""
# optimization at low theory
OPTIM_REM = f"""
$rem
{THEORY}
jobtype opt
geom_opt_max_cycles 200
$end"""
# FSM calculation
FSM_REM = f"""
$rem
{THEORY}
jobtype fsm
fsm_ngrad 10
fsm_nnode 20
fsm_mode 2
fsm_opt_mode 2
$end
"""
# TS opt from FSM calculation
TS_REM = f"""
$molecule
read
$end

$rem
{THEORY}
jobtype ts
scf_guess read
geom_opt_hessian read
geom_opt_dmax 50
geom_opt_max_cycles 100
$end
"""
# High level single points
HIGHLEVEL_SP_REM = f"""
$rem
{THEORY2}
jobtype sp
$end
"""


# --------------------------------------------------------------------------------- #
# Data analysis and input creation functions
def split_xyz(xyz_file: bytes) -> list[bytes]:
    """Split an xyz file into individual conformers."""
    lines = xyz_file.splitlines()
    structures = []
    while True:
        if len(lines) == 0:
            break

        # removed one deck
        natoms = lines.pop(0)
        n = int(natoms.decode())
        comment = lines.pop(0)
        geom = []
        for _ in range(n):
            geom += [lines.pop(0)]

        deck = b"\n".join([natoms, comment] + geom)
        structures += [deck]

    return structures


# --------------------------------------------------------------------------------- #
# Q-Chem stuff
def get_qchem_energy(output: bytes) -> bytes:
    """Extract SP energy from Q-Chem output."""
    for line in output.splitlines():
        if b"Total energy in the final basis set" in line:
            energy = float(line.decode().split("=")[1])
            return str(energy).encode()
    raise ValueError("No energy could be found in QChem output")


def check_if_converged(inp: bytes) -> bytes:
    """Check if a QChem optimization converged."""
    for line in inp.splitlines():
        if b"**  OPTIMIZATION CONVERGED  **" in line:
            return b"true"
    raise RuntimeError("Optimization failed")


def xyz2qchem(geom_xyz: bytes, charge: bytes, spin: bytes) -> bytes:
    """Transform xyz into QChem format."""
    lines = geom_xyz.decode().splitlines()
    natoms = int(lines[0].strip())
    _ = lines[1]
    geom = lines[2:]
    out = f"$molecule\n{charge.decode()} {spin.decode()}\n"
    for i in range(natoms):
        out += geom[i] + "\n"
    out += "$end\n"
    return out.encode()


# --------------------------------------------------------------------------------- #
# More functions
def make_unique_pairs(inp: bytes) -> list[bytes]:
    """Take a JSON list and return all unique pairs."""
    elements = json.loads(inp.decode())
    out = []
    for i in range(len(elements)):
        for j in range(i):
            out += [
                json.dumps(
                    {
                        "i": i,
                        "j": j,
                        "reactant": elements[i]["structure"].strip(),
                        "Ereactant": elements[i]["E"],
                        "product": elements[j]["structure"].strip(),
                        "Eproduct": elements[j]["E"],
                    }
                ).encode()
            ]
    return out


# TODO move this to utils also
def list2json(inp: list[Artefact]) -> Artefact:
    """Take a list of artefact encode it as JSON."""

    def __to_json(*dat: bytes) -> bytes:
        out = [d.decode() for d in dat]
        return json.dumps(out).encode()

    return f.reduce(__to_json, *inp)


def jsons2json(inp: list[Artefact]) -> Artefact:
    """Take a list of artefact JSONs and encode it as JSON."""

    def __to_json2(*dat: bytes) -> bytes:
        out = [json.loads(d.decode()) for d in dat]
        return json.dumps(out).encode()

    return f.reduce(__to_json2, *inp)


# --------------------------------------------------------------------------------- #
# Workflows
def high_level_sp(structure: Artefact) -> Artefact:
    """Compute SP energy at THEORY2."""
    deck = f.utils.concat(structure, HIGHLEVEL_SP_REM, join="\n")
    sp_calc = f.shell(
        "qchem -nt $SLURM_CPUS_PER_TASK sp2.in sp2.out sp2.scratch",
        inp={"sp2.in": deck},
        out=["sp2.out"],
        env={"QCSCRATCH": "."},
    )
    energy = f.morph(get_qchem_energy, sp_calc.out["sp2.out"])
    return energy


def optimize_conformer_xtb(structure: Artefact) -> Artefact:
    """Optimize a structure using xtb."""
    # perform an xtb optimization
    optim = f.shell(
        "OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK "
        + "MKL_NUM_THREADS=$SLURM_CPUS_PER_TASK "
        + "xtb input.xyz --opt vtight",
        inp={"input.xyz": structure},
        out=[".xtboptok", "xtbopt.xyz"],
    )

    # barrier for convergence
    struct, _ = f.utils.identity(optim.out["xtbopt.xyz"], optim.out[".xtboptok"])
    return struct


def optimize_conformer_dft(structure: Artefact) -> Artefact:
    # make a qchem file
    struct_qchem = f.reduce(xyz2qchem, structure, "0", "1")
    qchem_input = f.utils.concat(struct_qchem, OPTIM_REM, join="\n")

    # DFT optimization
    optim_dft = f.shell(
        "qchem -nt $SLURM_CPUS_PER_TASK sp.in sp.out sp.scratch",
        inp={"sp.in": qchem_input},
        out=["sp.out", "sp.scratch/molecule"],
        env={"QCSCRATCH": "."},
    )

    # barrier for convergence
    converged = f.morph(check_if_converged, optim_dft.out["sp.out"])
    dft, _ = f.utils.identity(optim_dft.out["sp.scratch/molecule"], converged)

    # high level SP
    E = high_level_sp(dft)

    def conformer_output(
        E: bytes,
        structure: bytes,
    ) -> bytes:
        """Save outputs to JSON."""
        data = {
            "E": E.decode(),
            "structure": structure.decode(),
        }
        return json.dumps(data).encode()

    return f.reduce(conformer_output, E, dft)


def get_ts_fsm(pair: Artefact) -> Artefact:
    """Workflow to do a FSM calculation on a paired structure."""

    # TODO: move that to utils too maybe
    def get_json_el(inp: bytes, key: bytes) -> bytes:
        output = json.loads(inp.decode())[key.decode()]
        if isinstance(output, str):
            return output.encode()
        else:
            return json.dumps(output).encode()

    reactant = f.reduce(get_json_el, pair, "reactant")
    product = f.reduce(get_json_el, pair, "product")

    molecule_deck = f.utils.concat(
        f.utils.truncate(reactant, bottom=1),
        "****",
        f.utils.truncate(product, top=2),
        join="\n",
    )
    qchem_input = f.utils.concat(molecule_deck, FSM_REM, join="\n")

    # FSM calculation
    fsm_calc = f.shell(
        "qchem -nt $SLURM_CPUS_PER_TASK sp2.in sp2.out sp2.scratch",
        "tar -czf scratch.tgz sp2.scratch",
        inp={"sp2.in": qchem_input},
        out=["sp2.out", "stringfile.txt", "scratch.tgz"],
        env={"QCSCRATCH": "."},
    )

    # TS calculation
    tsopt_calc = f.shell(
        "tar -xzf scratch.tgz",
        "qchem -nt $SLURM_CPUS_PER_TASK sp2.in sp2.out sp2.scratch",
        inp={"sp2.in": TS_REM, "scratch.tgz": fsm_calc.out["scratch.tgz"]},
        out=["sp2.out", "sp2.scratch/molecule"],
        env={"QCSCRATCH": "."},
    )
    # Raise if the TS hasn't converged
    converged = f.morph(check_if_converged, tsopt_calc.out["sp2.out"])
    ts, _ = f.utils.identity(tsopt_calc.out["sp2.scratch/molecule"], converged)

    # Now compute single point energies for everything at higher level theory
    Ets = high_level_sp(ts)

    def fsm_output(
        inp: bytes,
        Ets: bytes,
        ts: bytes,
        stringfile: bytes,
    ) -> bytes:
        """Save outputs to JSON."""
        inp_data = json.loads(inp.decode())
        inp_data.update(
            {"ts": ts.decode(), "Ets": Ets.decode(), "stringfile": stringfile.decode()}
        )
        return json.dumps(inp_data).encode()

    output = f.reduce(
        fsm_output,
        pair,
        Ets,
        ts,
        fsm_calc.out["stringfile.txt"],
    )

    return output


# --------------------------------------------------------------------------------- #
# Main program
with f.Fun():
    # -------------------------------------------------------------------------------- #
    # put smiles in db
    smiles = f.put("C(O)CC(O)")

    # -------------------------------------------------------------------------------- #
    # Generate 3d structures.
    gen3d = f.shell(
        "obabel input.smi --gen3d --ff mmff94 --minimize -O struct.mol",
        inp={"input.smi": smiles},
        out=["struct.mol"],
    )
    # abort if molecule is empty
    struct = not_empty(gen3d.out["struct.mol"])

    # -------------------------------------------------------------------------------- #
    # Generate conformers.
    confab = f.shell(
        "obabel input.mol -O conformers.xyz --confab --verbose",
        inp={"input.mol": struct},
        out=["conformers.xyz"],
    )

    # -------------------------------------------------------------------------------- #
    # Optimize conformer ensemble using xtb.
    optimized1 = f.dynamic.map_reduce(
        split_xyz,
        optimize_conformer_xtb,
        lambda x: f.utils.concat(*x, join="\n", strip=True),
        confab.out["conformers.xyz"],
    )

    # -------------------------------------------------------------------------------- #
    # Screen conformers.
    screen = f.shell(
        "crest conformers.xyz --cregen conformers.xyz",
        inp={"conformers.xyz": optimized1},
        out=["crest_ensemble.xyz"],
    )

    # -------------------------------------------------------------------------------- #
    # DFT optimization on each conformer.
    optimized = f.dynamic.map_reduce(
        split_xyz, optimize_conformer_dft, jsons2json, screen.out["crest_ensemble.xyz"]
    )

    # -------------------------------------------------------------------------------- #
    # Find TS by FSM for each pair of conformer.
    fsm_ts = f.dynamic.map_reduce(
        make_unique_pairs,
        get_ts_fsm,
        jsons2json,
        optimized,
    )
    f.execute(fsm_ts)

    print(f"input:  {smiles.hash[:6]}")
    print(f"output: {fsm_ts.hash[:6]}")
