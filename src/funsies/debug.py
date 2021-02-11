"""Helpful function for debugging workflows."""
# std
import os
import os.path
from typing import Optional

# external
from msgpack import unpackb
from redis import Redis

# module
from ._funsies import get_funsie
from ._shell import ShellOutput
from .constants import _AnyPath, hash_t
from .context import get_db
from .errors import UnwrapError
from .ui import take, takeout


# ----------------------------------------------------------------------
# Debugging functions
def shell(  # noqa:C901
    shell_output: ShellOutput, directory: _AnyPath, connection: Optional[Redis] = None
) -> None:
    """Extract all the files and outputs of a shell function to a directory."""
    errors = ""
    os.makedirs(directory, exist_ok=True)
    inp = os.path.join(directory, "input_files")
    out = os.path.join(directory, "output_files")
    db = get_db(connection)

    for key, val in shell_output.inp.items():
        try:
            p = os.path.join(inp, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            takeout(val, p, connection=db)
        except UnwrapError:
            errors += (
                f"input file:{key}:"
                + str(take(val, strict=False, connection=db))
                + "\n"
            )

    for key, val in shell_output.out.items():
        try:
            p = os.path.join(out, key)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            takeout(val, p, connection=db)
        except UnwrapError:
            errors += (
                f"output file:{key}:"
                + str(take(val, strict=False, connection=db))
                + "\n"
            )

    for i in range(len(shell_output.stdouts)):
        try:
            takeout(
                shell_output.stdouts[i],
                os.path.join(directory, f"stdout{i}"),
                connection=db,
            )
        except UnwrapError:
            errors += f"stdout{i}:" + str(take(val, strict=False, connection=db)) + "\n"

        try:
            takeout(
                shell_output.stderrs[i],
                os.path.join(directory, f"stderr{i}"),
                connection=db,
            )
        except UnwrapError:
            errors += f"stderr{i}:" + str(take(val, strict=False, connection=db)) + "\n"

    with open(os.path.join(directory, "error.log"), "w") as f:
        f.write(errors)

    with open(os.path.join(directory, "op.hash"), "w") as f:
        f.write(shell_output.hash)

    what = unpackb(get_funsie(db, shell_output.op.funsie).what)
    with open(os.path.join(directory, "op.sh"), "w") as f:
        f.write("\n".join(what["cmds"]))
        f.write("\n")

    if what["env"] is not None:
        with open(os.path.join(directory, "op.env"), "w") as f:
            for key, val in what["env"].items():
                f.write(f"{key}={val}\n")


# --------------
# Debug anything
def anything(
    hash: hash_t, output: _AnyPath, connection: Optional[Redis] = None
) -> None:
    """Output content of any hash object to a file."""
    pass
