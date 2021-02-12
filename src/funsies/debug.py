"""Helpful function for debugging workflows."""
from __future__ import annotations

# std
from dataclasses import asdict
import json
import os
import os.path
from typing import Optional, Union

# external
from msgpack import unpackb
from redis import Redis

# module
from ._funsies import Funsie, FunsieHow, get_funsie
from ._graph import Artefact, get_artefact, get_op, Operation
from ._shell import ShellOutput
from .constants import _AnyPath
from .context import get_db
from .errors import UnwrapError
from .ui import take, takeout


# ----------------------------------------------------------------------
# Debugging functions
def shell(  # noqa:C901
    shell_output: ShellOutput,
    directory: _AnyPath,
    connection: Optional[Redis[bytes]] = None,
) -> None:
    """Extract all the files and outputs of a shell function to a directory."""
    os.makedirs(directory, exist_ok=True)
    inp = os.path.join(directory, "input_files")
    out = os.path.join(directory, "output_files")
    db = get_db(connection)
    errors = {}

    for key, val in shell_output.inp.items():
        try:
            p = os.path.join(inp, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            takeout(val, p, connection=db)
        except UnwrapError:
            errors[f"input:{key}"] = asdict(take(val, strict=False, connection=db))

    for key, val in shell_output.out.items():
        try:
            p = os.path.join(out, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            takeout(val, p, connection=db)
        except UnwrapError:
            errors[f"output:{key}"] = asdict(take(val, strict=False, connection=db))

    for i in range(len(shell_output.stdouts)):
        try:
            takeout(
                shell_output.stdouts[i],
                os.path.join(directory, f"stdout{i}"),
                connection=db,
            )
        except UnwrapError:
            errors[f"stdout:{key}"] = asdict(take(val, strict=False, connection=db))

        try:
            takeout(
                shell_output.stderrs[i],
                os.path.join(directory, f"stderr{i}"),
                connection=db,
            )
        except UnwrapError:
            errors[f"stderr:{key}"] = asdict(take(val, strict=False, connection=db))

    with open(os.path.join(directory, "errors.json"), "w") as f:
        f.write(
            json.dumps(
                errors,
                sort_keys=True,
                indent=2,
            )
        )

    with open(os.path.join(directory, "operation.json"), "w") as f:
        f.write(json.dumps(asdict(shell_output.op), sort_keys=True, indent=2))

    what = unpackb(get_funsie(db, shell_output.op.funsie).what)
    with open(os.path.join(directory, "op.sh"), "w") as f:
        f.write("\n".join(what["cmds"]))
        f.write("\n")

    if what["env"] is not None:
        with open(os.path.join(directory, "op.env"), "w") as f:
            for key, val in what["env"].items():
                f.write(f"{key}={val}\n")


# --------------
# Debug artefact
def artefact(
    target: Artefact, directory: _AnyPath, connection: Optional[Redis[bytes]] = None
) -> None:
    """Output content of any hash object to a file."""
    db = get_db(connection)
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "metadata.json"), "w") as f:
        f.write(json.dumps(asdict(target), sort_keys=True, indent=2))
    try:
        takeout(
            target,
            os.path.join(directory, "data"),
            connection=db,
        )
    except UnwrapError:
        # dump error to json file
        with open(os.path.join(directory, "error.json"), "w") as f:
            f.write(
                json.dumps(
                    asdict(take(target, strict=False, connection=db)),
                    sort_keys=True,
                    indent=2,
                )
            )


def python(
    target: Union[Operation, Artefact],
    directory: _AnyPath,
    connection: Optional[Redis[bytes]] = None,
) -> None:
    """Output content of any hash object to a file."""
    db = get_db(connection)

    if isinstance(target, Artefact):
        # Get the corresponding operation
        target = get_op(db, target.parent)
        if target is None:
            raise RuntimeError(f"Operation not found at {target.parent}")

    os.makedirs(directory, exist_ok=True)
    funsie = get_funsie(db, target.funsie)
    inp = os.path.join(directory, "inputs")
    out = os.path.join(directory, "outputs")
    errors = {}
    if funsie.how != FunsieHow.python:
        raise RuntimeError(f"Operation is of type {funsie.how}, not a python function.")

    for key, v in target.inp.items():
        val = get_artefact(db, v)
        try:
            p = os.path.join(inp, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            takeout(val, p, connection=db)
        except UnwrapError:
            errors[f"input:{key}"] = asdict(take(val, strict=False, connection=db))

    for key, v in target.out.items():
        val = get_artefact(db, v)
        try:
            p = os.path.join(out, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            takeout(val, p, connection=db)
        except UnwrapError:
            errors[f"output:{key}"] = asdict(take(val, strict=False, connection=db))

    with open(os.path.join(directory, "errors.json"), "w") as f:
        f.write(
            json.dumps(
                errors,
                sort_keys=True,
                indent=2,
            )
        )

    meta = {
        "what": funsie.what.decode(),
        "inp": funsie.inp,
        "out": funsie.out,
        "error_tolerant": funsie.error_tolerant,
    }
    with open(os.path.join(directory, "funsie.json"), "w") as f:
        f.write(json.dumps(meta, sort_keys=True, indent=2))

    with open(os.path.join(directory, "operation.json"), "w") as f:
        f.write(json.dumps(asdict(target), sort_keys=True, indent=2))

    with open(os.path.join(directory, "function.pkl"), "wb") as f:
        assert funsie.aux is not None
        f.write(funsie.aux)


# --------------
# Debug anything
def anything(
    obj: Union[Artefact, Funsie, Operation, ShellOutput],
    output: _AnyPath,
    connection: Optional[Redis[bytes]] = None,
) -> None:
    """Debug anything really."""
    db = get_db(connection)
    if isinstance(obj, Operation):
        funsie = get_funsie(db, obj.funsie)
        if funsie.how == FunsieHow.shell:
            shell_output = ShellOutput(db, obj)
            shell(shell_output, output, db)
        elif funsie.how == FunsieHow.python:
            python(obj, output, db)
        else:
            raise RuntimeError()
    elif isinstance(obj, Artefact):
        artefact(obj, output, db)
    elif isinstance(obj, ShellOutput):
        shell(obj, output, db)
    else:
        raise NotImplementedError(f"Object of type {obj} cannot be debugged.")
