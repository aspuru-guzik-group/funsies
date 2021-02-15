"""Run shell commands using funsies."""
from __future__ import annotations

# std
import os
import subprocess
import tempfile
import time
from typing import Mapping, Optional, Sequence

# external
from msgpack import packb, unpackb
from redis import Redis

# module
from ._funsies import Funsie, FunsieHow
from ._graph import Artefact, get_artefact, Operation
from .constants import hash_t
from .errors import Error, Result
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
    env: Optional[dict[str, str]] = None,
    strict: bool = True,
) -> Funsie:
    """Wrap a shell command."""
    out = list(output_files)
    for k in range(len(cmds)):
        out += [f"{STDOUT}{k}", f"{STDERR}{k}", f"{RETURNCODE}{k}"]

    ierr = 1
    if strict:
        ierr = 0

    return Funsie(
        how=FunsieHow.shell,
        what=";".join(cmds),
        inp=list(input_files),
        out=out,
        error_tolerant=ierr,
        extra=dict(cmds=packb(list(cmds)), env=packb(env)),
    )


def run_shell_funsie(  # noqa:C901
    funsie: Funsie, input_values: Mapping[str, Result[bytes]]
) -> dict[str, Optional[bytes]]:
    """Execute a shell command."""
    logger.info("shell command")
    with tempfile.TemporaryDirectory() as dir:
        # Put in dir the input files
        for fn, val in input_values.items():
            if isinstance(val, Error):
                pass
            else:
                with open(os.path.join(dir, fn), "wb") as f:
                    f.write(val)

        cmds = unpackb(funsie.extra["cmds"])
        new_env = unpackb(funsie.extra["env"])

        env: Optional[dict[str, str]] = None
        if new_env is not None:
            env = os.environ.copy()
            env.update(new_env)

        out: dict[str, Optional[bytes]] = {}

        for k, c in enumerate(cmds):
            t1 = time.time()
            logger.info(f"{k+1}/{len(cmds)} $> {c}")
            proc = subprocess.run(c, cwd=dir, capture_output=True, shell=True, env=env)
            t2 = time.time()
            logger.info(f"done {k+1}/{len(cmds)} \t\tduration: {t2-t1:.2f}s")

            out[f"{STDOUT}{k}"] = proc.stdout
            out[f"{STDERR}{k}"] = proc.stderr
            out[f"{RETURNCODE}{k}"] = str(proc.returncode).encode()
            if proc.returncode:
                logger.warning(f"nonzero returncode={proc.returncode}")

        # Output files
        for file in funsie.out:
            if SPECIAL in file:
                pass
            else:
                try:
                    with open(os.path.join(dir, file), "rb") as f:
                        out[file] = f.read()
                except FileNotFoundError:
                    logger.warning(f"missing expected output {file}")
                    out[file] = None
    return out


# --------------------------------------------------------------------------------
# Convenience class for shell funsie
class ShellOutput:
    """A convenience wrapper for a shell operation."""

    op: Operation
    hash: hash_t
    out: dict[str, Artefact]
    inp: dict[str, Artefact]

    def __init__(self: "ShellOutput", store: Redis[bytes], op: Operation) -> None:
        """Generate a ShellOutput wrapper around a shell operation."""
        # import the constants
        from ._shell import SPECIAL, RETURNCODE, STDOUT, STDERR

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
                self.out[key] = get_artefact(store, val)

        self.inp = {}
        for key, val in op.inp.items():
            self.inp[key] = get_artefact(store, val)

        self.stdouts = []
        self.stderrs = []
        self.returncodes = []
        for i in range(self.n):
            self.stdouts += [get_artefact(store, op.out[f"{STDOUT}{i}"])]
            self.stderrs += [get_artefact(store, op.out[f"{STDERR}{i}"])]
            self.returncodes += [get_artefact(store, op.out[f"{RETURNCODE}{i}"])]

    def __check_len(self: "ShellOutput") -> None:
        if self.n > 1:
            raise AttributeError(
                "More than one shell command are included in this run."
            )

    @property
    def returncode(self: "ShellOutput") -> Artefact:
        """Return code of a shell command."""
        self.__check_len()
        return self.returncodes[0]

    @property
    def stdout(self: "ShellOutput") -> Artefact:
        """Stdout of a shell command."""
        self.__check_len()
        return self.stdouts[0]

    @property
    def stderr(self: "ShellOutput") -> Artefact:
        """Stderr of a shell command."""
        self.__check_len()
        return self.stderrs[0]
