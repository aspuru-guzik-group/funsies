"""Main data structures."""
# std
from dataclasses import asdict, dataclass
from enum import IntEnum
from typing import Dict, Literal, List, Optional, Type, Union

# external
from msgpack import packb, unpackb

# --------------------------------------------------------------------------------
class FunsieHow(IntEnum):
    """Kinds of funsies.

    This enum contains the various kinds of funsies: python code, shell code,
    etc.
    """

    python = 0
    shell = 1


@dataclass
class Funsie:
    """A funsie is a wrapped command that can be backed up to the KV store.

    A Funsie has a "how" (an integer that defines how it is to be executed), a
    "what" (a string that identifies the funsie, such as a function name or a
    shell command) and input and output artefacts. All of these are used to
    generate the hash of the Funsie instance.

    Funsies also have an aux field that include auxiliary data that is not to
    be used in key-ing the funsie, but is useful for executing it.
    """

    how: FunsieHow
    what: bytes
    inp: List[str]
    out: List[str]
    aux: Optional[bytes] = None

    def __str__(self: "Funsie") -> str:
        """Get the string representation of a funsie."""
        out = f"Funsie[\n  how={self.how}" + f"\n  what={self.what!r}" + "\n  inputs\n"
        for key in sorted(self.inp):
            out += f"    {key}\n"
        out += "  outputs\n"
        for key in sorted(self.out):
            out += f"    {key}\n"
        return out + "  ]"

    def pack(self: "Funsie") -> bytes:
        """Pack a Funsie to a bytestring."""
        return packb(asdict(self))

    @classmethod
    def unpack(cls: Type["Funsie"], data: bytes) -> "Funsie":
        """Unpack a Funsie from a byte string."""
        return Funsie(**unpackb(data))
