"""Helpful function for debugging workflows."""
from __future__ import annotations

# std
from dataclasses import asdict
import json
import os
import os.path
from typing import Any, Union

# module
from ._constants import _AnyPath
from ._context import Connection, get_connection
from ._funsies import Funsie, FunsieHow
from ._graph import Artefact, get_data, Operation
from ._shell import ShellOutput
from .errors import UnwrapError
from .ui import takeout


# ----------------------------------------------------------------------
# Debugging functions
def shell(  # noqa:C901
    shell_output: ShellOutput,
    directory: _AnyPath,
    connection: Connection = None,
) -> None:
    """Extract all the files and outputs of a shell function to a directory."""
    os.makedirs(directory, exist_ok=True)
    inp = os.path.join(directory, "input_files")
    out = os.path.join(directory, "output_files")
    db, store = get_connection(connection)
    errors = {}

    for key, val in shell_output.inp.items():
        try:
            p = os.path.join(inp, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            takeout(val, p, connection=(db, store))
        except UnwrapError:
            errors[f"input:{key}"] = asdict(get_data(db, store, val))

    for key, val in shell_output.out.items():
        try:
            p = os.path.join(out, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            takeout(val, p, connection=(db, store))
        except UnwrapError:
            errors[f"output:{key}"] = asdict(get_data(db, store, val))

    for i in range(len(shell_output.stdouts)):
        try:
            takeout(
                shell_output.stdouts[i],
                os.path.join(directory, f"stdout{i}"),
                connection=(db, store),
            )
        except UnwrapError:
            errors[f"stdout:{key}"] = asdict(get_data(db, store, val))

        try:
            takeout(
                shell_output.stderrs[i],
                os.path.join(directory, f"stderr{i}"),
                connection=(db, store),
            )
        except UnwrapError:
            errors[f"stderr:{key}"] = asdict(get_data(db, store, val))

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

    extra = Funsie.grab(db, shell_output.op.funsie).extra
    cmds = json.loads(extra["cmds"].decode())
    env = json.loads(extra["env"].decode())
    with open(os.path.join(directory, "op.sh"), "w") as f:
        f.write("\n".join(cmds))
        f.write("\n")

    if env is not None:
        with open(os.path.join(directory, "op.env"), "w") as f:
            for key, val in env.items():
                f.write(f"{key}={val}\n")


# --------------
# Debug artefact
def artefact(
    target: Artefact,
    directory: _AnyPath,
    connection: Connection = None,
) -> None:
    """Output content of any hash object to a file."""
    db, store = get_connection(connection)
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "metadata.json"), "w") as f:
        f.write(json.dumps(asdict(target), sort_keys=True, indent=2))
    try:
        takeout(
            target,
            os.path.join(directory, "data"),
            connection=(db, store),
        )
    except UnwrapError:
        # dump error to json file
        with open(os.path.join(directory, "error.json"), "w") as f:
            f.write(
                json.dumps(
                    asdict(get_data(db, store, target)),
                    sort_keys=True,
                    indent=2,
                )
            )


def python(
    target: Union[Operation, Artefact],
    directory: _AnyPath,
    connection: Connection = None,
) -> None:
    """Output content of any hash object to a file."""
    db, store = get_connection(connection)

    if isinstance(target, Artefact):
        # Get the corresponding operation
        target = Operation.grab(db, target.parent)
        if target is None:
            raise RuntimeError(f"Operation not found at {target.parent}")

    os.makedirs(directory, exist_ok=True)
    funsie = Funsie.grab(db, target.funsie)
    inp = os.path.join(directory, "inputs")
    out = os.path.join(directory, "outputs")
    errors = {}
    if funsie.how != FunsieHow.python:
        raise RuntimeError(f"Operation is of type {funsie.how}, not a python function.")

    for key, v in target.inp.items():
        val = Artefact[Any].grab(db, v)
        try:
            p = os.path.join(inp, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            takeout(val, p, connection=(db, store))
        except UnwrapError:
            errors[f"input:{key}"] = asdict(get_data(db, store, val))

    for key, v in target.out.items():
        val = Artefact.grab(db, v)
        try:
            p = os.path.join(out, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            takeout(val, p, connection=(db, store))
        except UnwrapError:
            errors[f"output:{key}"] = asdict(get_data(db, store, val))

    with open(os.path.join(directory, "errors.json"), "w") as f:
        f.write(
            json.dumps(
                errors,
                sort_keys=True,
                indent=2,
            )
        )

    meta = {
        "what": funsie.what,
        "inp": funsie.inp,
        "out": funsie.out,
        "error_tolerant": funsie.error_tolerant,
    }
    with open(os.path.join(directory, "funsie.json"), "w") as f:
        f.write(json.dumps(meta, sort_keys=True, indent=2))

    with open(os.path.join(directory, "operation.json"), "w") as f:
        f.write(json.dumps(asdict(target), sort_keys=True, indent=2))

    # TODO
    # with open(os.path.join(directory, "function.pkl"), "wb") as f:
    #     assert funsie.aux is not None
    #     f.write(funsie.aux)


# --------------
# Debug anything
def anything(
    obj: Union[Artefact, Funsie, Operation, ShellOutput],
    output: _AnyPath,
    connection: Connection = None,
) -> None:
    """Debug anything really."""
    db, store = get_connection(connection)
    if isinstance(obj, Operation):
        funsie = Funsie.grab(db, obj.funsie)
        if funsie.how == FunsieHow.shell:
            shell_output = ShellOutput(db, obj)
            shell(shell_output, output, connection=(db, store))
        elif funsie.how == FunsieHow.python:
            python(obj, output, connection=(db, store))
        else:
            raise RuntimeError()
    elif isinstance(obj, Artefact):
        artefact(obj, output, connection=(db, store))
    elif isinstance(obj, ShellOutput):
        shell(obj, output, connection=(db, store))
    else:
        raise NotImplementedError(f"Object of type {obj} cannot be debugged.")
