"""Run shell commands using funsies."""
# std
import logging
import os
import subprocess
import tempfile
from typing import Dict, Mapping, Optional, Sequence

# external
from msgpack import packb, unpackb

# module
from ._funsies import Funsie, FunsieHow

# Special namespaced "files"
SPECIAL = "__special__"
STDOUT = f"{SPECIAL}/stdout"
STDERR = f"{SPECIAL}/stderr"
RETURNCODE = f"{SPECIAL}/returncode"


def shell_funsie(
    cmds: Sequence[str],
    input_files: Sequence[str],
    output_files: Sequence[str],
    env: Optional[Dict[str, str]] = None,
) -> Funsie:
    """Wrap a shell command."""
    out = list(output_files)
    for k in range(len(cmds)):
        out += [f"{STDOUT}{k}", f"{STDERR}{k}", f"{RETURNCODE}{k}"]

    return Funsie(
        how=FunsieHow.shell,
        what=packb({"cmds": cmds, "env": env}),
        inp=list(input_files),
        out=out,
    )


def run_shell_funsie(  # noqa:C901
    funsie: Funsie,
    input_values: Mapping[str, Optional[bytes]],
) -> Dict[str, Optional[bytes]]:
    """Execute a shell command."""
    # TODO expandvar, expandusr for tempdir
    # TODO setable tempdir
    with tempfile.TemporaryDirectory(dir=None) as dir:
        # Put in dir the input files
        for fn in funsie.inp:
            if fn not in input_values:
                logging.error(f"Missing data for arg {fn}!")
                val = None
            else:
                val = input_values[fn]

            val = input_values[fn]
            if val is not None:
                with open(os.path.join(dir, fn), "wb") as f:
                    f.write(val)
            else:
                logging.warning(f"file {fn} not present.")

        shell = unpackb(funsie.what)

        # goto shell funsie above for definitions of those.
        cmds = shell["cmds"]
        env = shell["env"]
        out: Dict[str, Optional[bytes]] = {}

        for k, c in enumerate(cmds):
            try:
                proc = subprocess.run(
                    c,
                    cwd=dir,
                    capture_output=True,
                    env=env,
                    shell=True,
                )
            except Exception as e:
                logging.exception(f"run_command failed with exception {e}")
                out[f"{STDOUT}{k}"] = None
                out[f"{STDERR}{k}"] = None
                out[f"{RETURNCODE}{k}"] = None
            else:
                out[f"{STDOUT}{k}"] = proc.stdout
                out[f"{STDERR}{k}"] = proc.stderr
                out[f"{RETURNCODE}{k}"] = str(proc.returncode).encode()

        # Output files
        for file in funsie.out:
            if SPECIAL in file:
                pass
            else:
                try:
                    with open(os.path.join(dir, file), "rb") as f:
                        out[file] = f.read()
                except FileNotFoundError:
                    logging.warning(f"expected file {file}, but didn't find it")
                    out[file] = None
    return out
