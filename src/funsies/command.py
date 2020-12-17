"""CLI command runner."""
# std
import logging
import subprocess
from typing import Dict, Optional, Union

# module
from .constants import _AnyPath
from .types import Command, CommandOutput, SavedCommand


def run_command(
    dir: _AnyPath,
    environ: Optional[Dict[str, str]],
    command: Union[Command, SavedCommand],
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
