"""User-friendly interfaces to funsies functionality."""
from __future__ import annotations

# std
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    Optional,
    overload,
    Sequence,
    TypeVar,
    Union,
)

# external
from mypy_extensions import VarArg
from redis import Redis

# python 3.7 imports Literal from typing_extensions
try:
    # std
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type:ignore

# module
from ._constants import _AnyPath, _Data, Encoding
from ._context import get_db, get_options
from ._graph import Artefact, constant_artefact, make_op
from ._infer import output_types
from ._pyfunc import python_funsie
from .config import Options
from .errors import Result

# Types
_Target = Union[Artefact, _Data]
_INP_FILES = Optional[Mapping[_AnyPath, _Target]]
_OUT_FILES = Optional[Iterable[_AnyPath]]
T = TypeVar("T")

# Yay overloads! we all wish there were an easier way of doing this but here we are...
Tin1 = TypeVar("Tin1", bound=_Data)
Tin2 = TypeVar("Tin2", bound=_Data)
Tin3 = TypeVar("Tin3", bound=_Data)
Tin4 = TypeVar("Tin4", bound=_Data)
Tout1 = TypeVar("Tout1", bound=_Data)
Tout2 = TypeVar("Tout2", bound=_Data)
Tout3 = TypeVar("Tout3", bound=_Data)
Tout4 = TypeVar("Tout4", bound=_Data)
_inp = Union[Tin1, Artefact[Tin1]]

# This is like C++ templates but worse.

# out: Optional[Encoding] = None,
# name: Optional[str] = None,
# strict: bool = True,
# opt: Optional[Options] = None,
# connection: Optional[Redis[bytes]] = None,

# -------------------------------------------------------------------------------------------------------------------- #
# REDUCE
# fmt:off
@overload
def reduce(fun: Callable[[Tin1], Tout1], __inp1: _inp[Tin1], *, out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[True]= ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def reduce(fun: Callable[[Tin1, Tin2], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], *, out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[True]= ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def reduce(fun: Callable[[Tin1, Tin2, Tin3], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], *, out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[True]= ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def reduce(fun: Callable[[Tin1, Tin2, Tin3, Tin4], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], __inp4: _inp[Tin4], *, out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[True]= ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def reduce(fun: Callable[[VarArg(Tin1)], Tout1], *__inp1: _inp[Tin1], out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[True]= ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def reduce(fun: Callable[[Result[Tin1]], Tout1], __inp1: _inp[Tin1], *, out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[False]= ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def reduce(fun: Callable[[Result[Tin1], Result[Tin2]], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], *, out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[False]= ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def reduce(fun: Callable[[Result[Tin1], Result[Tin2], Result[Tin3]], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], *, out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[False]= ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def reduce(fun: Callable[[Result[Tin1], Result[Tin2], Result[Tin3], Result[Tin4]], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], __inp4: _inp[Tin4], *, out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[False]= ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def reduce(fun: Callable[[VarArg(Result[Tin1])], Tout1], *__inp1: _inp[Tin1], out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[False]= ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...

# -------------------------------------------------------------------------------------------------------------------- #
# MORPH
@overload
def morph(fun: Callable[[Tin1], Tout1], inp: Union[Tin1, Artefact[Tin1]], out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[True] = ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def morph(fun: Callable[[Result[Tin1]], Tout1], inp: Union[Tin1, Artefact[Tin1]], out:Optional[Encoding]=..., name:Optional[str]=..., strict: Literal[False] = ..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...

# -------------------------------------------------------------------------------------------------------------------- #
# PY -> 5 input overload, 4 output overload, strictness = 40 overloads (nice)
# fmt:off
# out1
@overload
def py(fun: Callable[[Tin1], Tout1], __inp1: _inp[Tin1], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def py(fun: Callable[[Tin1, Tin2], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def py(fun: Callable[[Tin1, Tin2, Tin3], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def py(fun: Callable[[Tin1, Tin2, Tin3, Tin4], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], __inp4: _inp[Tin4], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def py(fun: Callable[[VarArg(Tin1)], Tout1], *__inp1: _inp[Tin1], out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...

# out2
@overload
def py(fun: Callable[[Tin1], tuple[Tout1, Tout2]], __inp1: _inp[Tin1], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2]]: ...
@overload
def py(fun: Callable[[Tin1, Tin2], tuple[Tout1, Tout2]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2]]: ...
@overload
def py(fun: Callable[[Tin1, Tin2, Tin3], tuple[Tout1, Tout2]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2]]: ...
@overload
def py(fun: Callable[[Tin1, Tin2, Tin3, Tin4], tuple[Tout1, Tout2]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], __inp4: _inp[Tin4], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2]]: ...
@overload
def py(fun: Callable[[VarArg(Tin1)], tuple[Tout1, Tout2]], *__inp1: _inp[Tin1], out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2]]: ...

# out3
@overload
def py(fun: Callable[[Tin1], tuple[Tout1, Tout2,Tout3]], __inp1: _inp[Tin1], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3]]: ...
@overload
def py(fun: Callable[[Tin1, Tin2], tuple[Tout1, Tout2,Tout3]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3]]: ...
@overload
def py(fun: Callable[[Tin1, Tin2, Tin3], tuple[Tout1, Tout2,Tout3]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3]]: ...
@overload
def py(fun: Callable[[Tin1, Tin2, Tin3, Tin4], tuple[Tout1, Tout2,Tout3]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], __inp4: _inp[Tin4], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3]]: ...
@overload
def py(fun: Callable[[VarArg(Tin1)], tuple[Tout1, Tout2,Tout3]], *__inp1: _inp[Tin1], out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3]]: ...

# out4
@overload
def py(fun: Callable[[Tin1], tuple[Tout1, Tout2,Tout3,Tout4]], __inp1: _inp[Tin1], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3],Artefact[Tout4]]: ...
@overload
def py(fun: Callable[[Tin1, Tin2], tuple[Tout1, Tout2,Tout3,Tout4]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3],Artefact[Tout4]]: ...
@overload
def py(fun: Callable[[Tin1, Tin2, Tin3], tuple[Tout1, Tout2,Tout3,Tout4]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3],Artefact[Tout4]]: ...
@overload
def py(fun: Callable[[Tin1, Tin2, Tin3, Tin4], tuple[Tout1, Tout2,Tout3,Tout4]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], __inp4: _inp[Tin4], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3],Artefact[Tout4]]: ...
@overload
def py(fun: Callable[[VarArg(Tin1)], tuple[Tout1, Tout2,Tout3,Tout4]], *__inp1: _inp[Tin1], out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict: Literal[True]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3],Artefact[Tout4]]: ...

# STRICT VERSION
# out1
@overload
def py(fun: Callable[[Result[Tin1]], Tout1], __inp1: _inp[Tin1], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2]], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2], Result[Tin3]], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2], Result[Tin3], Result[Tin4]], Tout1], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], __inp4: _inp[Tin4], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...
@overload
def py(fun: Callable[[VarArg(Result[Tin1])], Tout1], *__inp1: _inp[Tin1], out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> Artefact[Tout1]: ...

# out2
@overload
def py(fun: Callable[[Result[Tin1]], tuple[Tout1, Tout2]], __inp1: _inp[Tin1], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2]]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2]], tuple[Tout1, Tout2]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2]]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2], Result[Tin3]], tuple[Tout1, Tout2]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2]]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2], Result[Tin3], Result[Tin4]], tuple[Tout1, Tout2]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], __inp4: _inp[Tin4], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2]]: ...
@overload
def py(fun: Callable[[VarArg(Result[Tin1])], tuple[Tout1, Tout2]], *__inp1: _inp[Tin1], out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2]]: ...

# out3
@overload
def py(fun: Callable[[Result[Tin1]], tuple[Tout1, Tout2,Tout3]], __inp1: _inp[Tin1], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3]]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2]], tuple[Tout1, Tout2,Tout3]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3]]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2], Result[Tin3]], tuple[Tout1, Tout2,Tout3]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3]]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2], Result[Tin3], Result[Tin4]], tuple[Tout1, Tout2,Tout3]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], __inp4: _inp[Tin4], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3]]: ...
@overload
def py(fun: Callable[[VarArg(Result[Tin1])], tuple[Tout1, Tout2,Tout3]], *__inp1: _inp[Tin1], out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3]]: ...

# out4
@overload
def py(fun: Callable[[Result[Tin1]], tuple[Tout1, Tout2,Tout3,Tout4]], __inp1: _inp[Tin1], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3],Artefact[Tout4]]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2]], tuple[Tout1, Tout2,Tout3,Tout4]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3],Artefact[Tout4]]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2], Result[Tin3]], tuple[Tout1, Tout2,Tout3,Tout4]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3],Artefact[Tout4]]: ...
@overload
def py(fun: Callable[[Result[Tin1], Result[Tin2], Result[Tin3], Result[Tin4]], tuple[Tout1, Tout2,Tout3,Tout4]], __inp1: _inp[Tin1], __inp2: _inp[Tin2], __inp3: _inp[Tin3], __inp4: _inp[Tin4], *, out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3],Artefact[Tout4]]: ...
@overload
def py(fun: Callable[[VarArg(Result[Tin1])], tuple[Tout1, Tout2,Tout3,Tout4]], *__inp1: _inp[Tin1], out:Optional[Sequence[Encoding]]=..., name:Optional[str]=..., strict:Literal[False]=..., opt:Optional[Options]=..., connection:Optional[Redis[bytes]]=...,) -> tuple[Artefact[Tout1],Artefact[Tout2],Artefact[Tout3],Artefact[Tout4]]: ...
