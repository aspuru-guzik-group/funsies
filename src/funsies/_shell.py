"""Run shell commands using funsies."""
# std
import logging
import os
import subprocess
import tempfile
from typing import Dict, Optional, Sequence

# external
from msgpack import packb, unpackb

# module
from ._funsies import Funsie, FunsieHow

# Special namespaced "files"
SPECIAL = "__special__"
STDOUT = f"{SPECIAL}/stdout"
STDERR = f"{SPECIAL}/stderr"
RETURNCODE = f"{SPECIAL}/returncode"


class ShellOutput:
    """A convenience wrapper for a shell operation."""

    def __init__(self, op):
        """Generate a ShellOutput wrapper around a shell operation."""
        # stuff that is the same
        self.op = op
        self.hash = op.hash

        self.out = {}
        self.n = 0
        for key, val in op.out.items():
            if SPECIAL in key:
                if RETURNCODE in key:
                    self.n += 1  # count the number of commands
            else:
                self.out[key] = val

            self.out = op.out
        self.inp = op.inp

        self.stdouts = []
        self.stderrs = []
        self.returncodes = []
        for i in range(self.n):
            self.stdouts += [op.out[f"{STDOUT}{i}"]]
            self.stderrs += [op.out[f"{STDERR}{i}"]]
            self.returncodes += [op.out[f"{RETURNCODE}{i}"]]

    def __warn_len(self):
        if self.n > 1:
            raise AttributeError(
                "More than one shell command are included in this run."
            )

    @property
    def returncode(self):
        self.__warn_len()
        return self.returncodes[0]

    @property
    def stdout(self):
        self.__warn_len()
        return self.stdouts[0]

    @property
    def stderr(self):
        self.__warn_len()
        return self.stderr[0]


def shell_funsie(
    cmds: Sequence[str],
    input_files: Sequence[str],
    output_files: Sequence[str],
    env: Optional[Dict[str, str]] = None,
) -> Funsie:
    """Wrap a shell command."""
    out = output_files
    for k in range(len(cmds)):
        out += [f"{STDOUT}{k}", f"{STDERR}{k}", f"{RETURNCODE}{k}"]

    return Funsie(
        how=FunsieHow.shell,
        what=packb({"cmds": cmds, "env": env}),
        inp=input_files,
        out=out,
    )


def run_shell_funsie(
    funsie: Funsie,
    input_values: Dict[str, Optional[bytes]],
) -> Dict[str, Optional[bytes]]:  # noqa:C901
    """Execute a shell command."""
    # TODO expandvar, expandusr for tempdir
    # TODO setable tempdir
    with tempfile.TemporaryDirectory(dir=None) as dir:
        # Put in dir the input files
        for fn, val in input_values.items():
            with open(os.path.join(dir, fn), "wb") as f:
                # load the artefact
                if val is not None:
                    f.write(val)
                else:
                    logging.warning(f"file {fn} not there.")

        shell = unpackb(funsie.what)

        # goto shell funsie above for definitions of those.
        cmds = shell["cmds"]
        env = shell["env"]
        out = {}

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
                return False

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
