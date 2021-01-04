"""Main data structures."""
# std
from dataclasses import asdict, dataclass
from enum import IntEnum
import hashlib
from typing import Dict, List, Literal, Optional, Type, Union

# external
from msgpack import packb, unpackb

# --------------------------------------------------------------------------------
# Constants
ART_TYPES = ["bytes", "str", "int"]
_ART_TYPES = Union[Literal["bytes"], Literal["str"], Literal["int"]]


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
    what: str
    inp: Dict[str, _ART_TYPES]
    out: Dict[str, _ART_TYPES]
    aux: Optional[bytes] = None

    def __str__(self: "Funsie") -> str:
        """Get the string representation of a funsie."""
        out = f"Funsie[\n  how={self.how}" + f"\n  what={self.what}" + "\n  inputs\n"
        for key, val in sorted(self.inp.items(), key=lambda x: x[0]):
            out += f"    {key} = {val}\n"
        out += "  outputs\n"
        for key, val in sorted(self.out.items(), key=lambda x: x[0]):
            out += f"    {key} = {val}\n"
        return out + "  ]"

    def hash(self: "Funsie") -> str:
        """Compute the hash value of a Funsie."""
        m = hashlib.sha256()
        m.update(str(self).encode())
        result = m.hexdigest()
        return result

    def pack(self: "Funsie") -> bytes:
        """Pack a Funsie to a bytestring."""
        return packb(asdict(self))

    @classmethod
    def unpack(cls: Type["Funsie"], data: bytes) -> "Funsie":
        """Unpack a Funsie from a byte string."""
        return Funsie(**unpackb(data))


# --------------------------------------------------------------------------------
# Graph
def store_artefact(store, value):
    what = type(value).__name__
    if what not in ART_TYPES:
        raise TypeError(f"Type {what} not in {ART_TYPES}")


def add_funsie(kv):
    pass
