"""CLI command wrappers."""
# std
from dataclasses import dataclass, field
import logging
import subprocess
from typing import Any, Dict, List, Optional, Type, Union

# module
from .cached import FilePtr
from .constants import _AnyPath


# ------------------------------------------------------------------------------
# Types for Commands
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
    stdout: FilePtr
    stderr: FilePtr

    # Maybe we just want to get rid of these classes altogether.
    @classmethod
    def from_dict(
        cls: Type["CachedCommandOutput"], c: Dict[str, Any]
    ) -> "CachedCommandOutput":
        """Populate from a dictionary."""
        return CachedCommandOutput(
            returncode=c["returncode"],
            executable=c["executable"],
            args=c["args"],
            stdout=FilePtr(**c["stdout"]),
            stderr=FilePtr(**c["stderr"]),
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
