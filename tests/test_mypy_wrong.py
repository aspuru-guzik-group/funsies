import funsies
from funsies.types import Artefact


def fun1(a: str) -> bytes:
    ...


def fun2(a: str, b: bytes) -> bytes:
    ...


def fun3(*a: str) -> int:
    ...


astr: Artefact[str]
abyt: Artefact[bytes]

# bad
out = funsies.reduce(fun1, "bla", "bla")
out = funsies.reduce(fun2, "bla", "bla")
out = funsies.reduce(fun2, "bla")

out = funsies.reduce(fun2, astr)
out = funsies.reduce(fun2, astr, astr)
out = funsies.reduce(fun1, astr, astr)
out = funsies.reduce(fun1, abyt)

out = funsies.reduce(fun3, astr, astr, astr, astr, astr, abyt)

# ok
funsies.reduce(fun1, "bla")
funsies.reduce(fun2, "bla", b"ok")
funsies.reduce(fun2, astr, abyt)
funsies.reduce(fun3, astr, astr, astr, astr, astr)
