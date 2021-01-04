"""Run shell commands using funsies."""
# std
from typing import Dict, Sequence

# external
from msgpack import packb

# module
from ._funsies import _ART_TYPES, Funsie, FunsieHow


def wrap_shell(
    cmds: Sequence[str], input_files: Sequence[str], output_files: Sequence[str]
) -> Funsie:
    """Wrap a shell command."""
    out: Dict[str, _ART_TYPES] = dict([(k, "bytes") for k in output_files])

    for k in range(len(cmds)):
        out[f"__stdout{k}"] = "bytes"
        out[f"__stderr{k}"] = "bytes"
        out[f"__returncode{k}"] = "int"

    return Funsie(
        how=FunsieHow.shell,
        what="\n".join(cmds),
        inp=dict([(k, "bytes") for k in input_files]),
        out=out,
        aux=packb(cmds),
    )
