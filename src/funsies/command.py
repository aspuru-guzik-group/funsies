"""CLI command wrappers."""
# std
from dataclasses import asdict, dataclass, field
import json
import logging
import subprocess
from typing import Dict, List, Optional, Type, Union

# module
from .cached import CachedFile
from .constants import _AnyPath


# ------------------------------------------------------------------------------
# Types for Tasks and Commands
@dataclass
class Command:
    """A shell command executed by a task."""

    executable: str
    args: List[str] = field(default_factory=list)

    def __repr__(self: "Command") -> str:
        """Return command as a string."""
        return self.executable + " " + " ".join([a for a in self.args])


@dataclass
class CommandOutput:
    """Holds the result of running a command."""

    returncode: int
    stdout: Optional[bytes]
    stderr: Optional[bytes]
    raises: Optional[Exception] = None


@dataclass
class CachedCommandOutput:
    """Holds the result of running a command, with its stdout and err cached.."""

    returncode: int
    executable: str
    args: List[str]
    stdout: CachedFile
    stderr: CachedFile

    def json(self: "CachedCommandOutput") -> str:
        """Return a json version of myself."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls: Type["CachedCommandOutput"], inp: str) -> "CachedCommandOutput":
        """Make a CachedCommandOutput from a json string."""
        d = json.loads(inp)
        return CachedCommandOutput(
            returncode=d["returncode"],
            executable=d["executable"],
            args=d["args"],
            stdout=CachedFile(**d["stdout"]),
            stderr=CachedFile(**d["stderr"]),
        )


def run_command(
    dir: _AnyPath,
    environ: Optional[Dict[str, str]],
    command: Union[Command, CachedCommandOutput],
) -> CommandOutput:
    """Run a Command."""
    args = [command.executable] + [a for a in command.args]
    error = None

    try:
        proc = subprocess.run(args, cwd=dir, capture_output=True, env=environ)
    except Exception as e:
        logging.exception("run_command failed with exception")
        return CommandOutput(-1, b"", b"", e)

    return CommandOutput(
        proc.returncode, stdout=proc.stdout, stderr=proc.stderr, raises=error
    )
