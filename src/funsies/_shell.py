"""Run shell commands using funsies."""
from __future__ import annotations

# std
from io import BytesIO
import json
import os
import shutil
import subprocess
import tempfile
import time
from typing import Any, Mapping, Optional, Sequence

# external
from redis import Redis

# module
from ._constants import _Data, Encoding, hash_t
from ._funsies import Funsie, FunsieHow
from ._graph import Artefact, Operation
from ._logging import logger
from .errors import Error, Result

# Special namespaced "files"
SPECIAL = "__special__"
STDOUT = f"{SPECIAL}/stdout"
STDERR = f"{SPECIAL}/stderr"
RETURNCODE = f"{SPECIAL}/returncode"


def shell_funsie(
    cmds: Sequence[str],
    input_files: dict[str, Encoding],
    output_files: Sequence[str],
    env: Optional[dict[str, str]] = None,
    strict: bool = True,
) -> Funsie:
    """Wrap a shell command."""
    out = {}
    for fn in output_files:
        out[fn] = Encoding.blob
        # TODO: files that end with .json

    for k in range(len(cmds)):
        out[f"{STDOUT}{k}"] = Encoding.blob
        out[f"{STDERR}{k}"] = Encoding.blob
        out[f"{RETURNCODE}{k}"] = Encoding.json

    ierr = 1
    if strict:
        ierr = 0

    extra = dict(cmds=json.dumps(list(cmds)).encode(), env=json.dumps(env).encode())

    return Funsie(
        how=FunsieHow.shell,
        what=" && ".join(cmds),
        inp=input_files,
        out=out,
        error_tolerant=ierr,
        extra=extra,
    )


def run_shell_funsie(  # noqa:C901
    funsie: Funsie, input_values: Mapping[str, Result[BytesIO]]
) -> dict[str, Optional[_Data]]:
    """Execute a shell command."""
    logger.info("shell command")
    with tempfile.TemporaryDirectory() as dir:
        # Put in dir the input files
        for fn, val in input_values.items():
            if isinstance(val, Error):
                pass
            else:
                with open(os.path.join(dir, fn), "wb") as f:
                    shutil.copyfileobj(val, f)

        cmds = json.loads(funsie.extra["cmds"].decode())
        new_env = json.loads(funsie.extra["env"].decode())
        env: Optional[dict[str, str]] = None
        if new_env:
            env = os.environ.copy()
            env.update(new_env)

        out: dict[str, Optional[_Data]] = {}

        for k, c in enumerate(cmds):
            t1 = time.time()
            logger.info(f"{k+1}/{len(cmds)} $> {c}")
            proc = subprocess.run(c, cwd=dir, capture_output=True, shell=True, env=env)
            t2 = time.time()
            logger.info(f"done {k+1}/{len(cmds)} \t\tduration: {t2-t1:.2f}s")

            out[f"{STDOUT}{k}"] = proc.stdout
            out[f"{STDERR}{k}"] = proc.stderr
            out[f"{RETURNCODE}{k}"] = int(proc.returncode)
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
    out: dict[str, Artefact[bytes]]
    inp: dict[str, Artefact[Any]]

    def __init__(self: "ShellOutput", store: Redis[bytes], op: Operation) -> None:
        """Generate a ShellOutput wrapper around a shell operation."""
        # import the constants
        # module
        from ._shell import RETURNCODE, SPECIAL, STDERR, STDOUT

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
                self.out[key] = Artefact[bytes].grab(store, val)

        self.inp = {}
        for key, val in op.inp.items():
            self.inp[key] = Artefact[Any].grab(store, val)

        self.stdouts = []
        self.stderrs = []
        self.returncodes = []
        for i in range(self.n):
            self.stdouts += [Artefact[bytes].grab(store, op.out[f"{STDOUT}{i}"])]
            self.stderrs += [Artefact[bytes].grab(store, op.out[f"{STDERR}{i}"])]
            self.returncodes += [Artefact[int].grab(store, op.out[f"{RETURNCODE}{i}"])]

    def __check_len(self: "ShellOutput") -> None:
        if self.n > 1:
            raise AttributeError(
                "More than one shell command are included in this run."
            )

    @property
    def returncode(self: "ShellOutput") -> Artefact[int]:
        """Return code of a shell command."""
        self.__check_len()
        return self.returncodes[0]

    @property
    def stdout(self: "ShellOutput") -> Artefact[bytes]:
        """Stdout of a shell command."""
        self.__check_len()
        return self.stdouts[0]

    @property
    def stderr(self: "ShellOutput") -> Artefact[bytes]:
        """Stderr of a shell command."""
        self.__check_len()
        return self.stderrs[0]
