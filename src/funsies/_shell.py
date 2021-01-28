"""Run shell commands using funsies."""
# std
import os
import subprocess
import tempfile
from typing import Dict, Mapping, Optional, Sequence

# external
from msgpack import packb, unpackb

# module
from ._funsies import Funsie, FunsieHow
from .errors import Result
from .logging import logger

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
    strict: bool = True,
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
        error_tolerant=not strict,
    )


def run_shell_funsie(  # noqa:C901
    funsie: Funsie,
    input_values: Mapping[str, Result[bytes]],
) -> Dict[str, Optional[bytes]]:
    """Execute a shell command."""
    # TODO expandvar, expandusr for tempdir
    # TODO setable tempdir
    with tempfile.TemporaryDirectory(dir=None) as dir:
        # Put in dir the input files
        incoming_files, _ = funsie.check_inputs(input_values)
        for fn, val in incoming_files.items():
            with open(os.path.join(dir, fn), "wb") as f:
                f.write(val)

        shell = unpackb(funsie.what)

        # goto shell funsie above for definitions of those.
        cmds = shell["cmds"]
        env = shell["env"]
        out: Dict[str, Optional[bytes]] = {}

        for k, c in enumerate(cmds):
            proc = subprocess.run(
                c,
                cwd=dir,
                capture_output=True,
                env=env,
                shell=True,
            )

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
                    logger.warning(f"expected file {file}, but didn't find it")
                    out[file] = None
    return out
