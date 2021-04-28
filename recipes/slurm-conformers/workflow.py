"""Getting conformers of molecules using xtb.

This is an example workflow."""
from __future__ import annotations

from typing import Sequence, Any


import funsies as f
from funsies.types import Artefact, Encoding
from funsies.utils import not_empty


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


def optimize_conformer_xtb(structure: Artefact[bytes]) -> Artefact[bytes]:
    """Optimize a structure using xtb."""
    # perform an xtb optimization
    optim = f.shell(
        "xtb input.xyz --opt vtight",
        inp={"input.xyz": structure},
        out=[".xtboptok", "xtbopt.xyz"],
    )
    # barrier for convergence (fails if .xtboptok is not found)
    struct = f.utils.reduce(
        lambda x, y: x, optim.out["xtbopt.xyz"], optim.out[".xtboptok"]
    )
    return struct


def make_data_output(structures: Sequence[Artefact[bytes]]) -> Artefact[list[Any]]:
    """Take xyz structure from xtb and parse them to a list of dicts."""

    def to_dict(xyz: bytes) -> dict[str, Any]:
        as_str = xyz.decode().strip()
        energy = float(as_str.splitlines()[1].split()[1])
        return {"structure": as_str, "energy": energy}

    def sort_by_energy(*elements: dict[str, Any]) -> list[dict[str, Any]]:
        out = [el for el in elements]
        out = sorted(out, key=lambda x: x["energy"])
        return out

    out = []
    for s in structures:
        out += [f.morph(to_dict, s, out=Encoding.json)]  # elements to dicts
    return f.reduce(sort_by_energy, *out)  # transform to a sorted list


with f.Fun():
    # put smiles in db
    smiles = f.put(b"C(O)CCCC(O)")

    # Generate 3d conformers with openbabel
    gen3d = f.shell(
        "obabel input.smi --gen3d --ff mmff94 --minimize -O struct.mol",
        inp={"input.smi": smiles},
        out=["struct.mol"],
    )
    # abort if molecule is empty
    struct = not_empty(gen3d.out["struct.mol"])

    # Generate conformers.
    confab = f.shell(
        "obabel input.mol -O conformers.xyz --confab --verbose",
        inp={"input.mol": struct},
        out=["conformers.xyz"],
    )

    # Optimize conformer ensemble using xtb.
    optimized1 = f.dynamic.sac(
        # split the xyz file
        split_xyz,
        # optimize the conformers
        optimize_conformer_xtb,
        # join the structures
        make_data_output,
        # input data:
        confab.out["conformers.xyz"],
        # output is json
        out=Encoding.json,
    )

    # execute the workflow
    f.execute(optimized1)
    # block until its over
    f.wait_for(optimized1)
    # dump results for analysis
    f.takeout(optimized1, "conformers.json")
