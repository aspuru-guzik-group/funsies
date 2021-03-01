# import funsies
# from funsies.types import Artefact, Result


# def fun1(a: str) -> bytes:
#     ...


# def fun2(a: str, b: bytes) -> bytes:
#     ...


# def fun2r(a: Result[str], b: Result[bytes]) -> bytes:
#     ...


# def fun3(*a: str) -> int:
#     ...


# astr: Artefact[str]
# abyt: Artefact[bytes]

# # bad
# funsies.reduce(fun1, "bla", "bla")
# funsies.reduce(fun2, "bla", "bla")
# funsies.reduce(fun2, "bla")

# funsies.reduce(fun2, astr)
# funsies.reduce(fun2, astr, astr)
# funsies.reduce(fun1, astr, astr)
# funsies.reduce(fun1, abyt)

# funsies.reduce(fun3, astr, astr, astr, astr, astr, abyt)


# funsies.reduce(fun2, astr, abyt, strict=False)

# # ok
# funsies.reduce(fun1, "bla")
# funsies.reduce(fun2, "bla", b"ok")
# funsies.reduce(fun2, astr, abyt)
# funsies.reduce(fun3, astr, astr, astr, astr, astr)

# funsies.reduce(fun2r, astr, abyt, strict=False)
