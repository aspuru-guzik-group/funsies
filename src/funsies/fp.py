"""User-friendly interfaces to funsies functionality."""
from __future__ import annotations

# std
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence, TypeVar, Union

# external
from redis import Redis

# module
from ._constants import _AnyPath, _Data, Encoding
from ._context import Connection, get_connection, get_options
from ._graph import Artefact, constant_artefact, make_op
from ._infer import output_types
from ._pyfunc import python_funsie
from .config import Options, StorageEngine

# Types
_Target = Union[Artefact, _Data]
_INP_FILES = Optional[Mapping[_AnyPath, _Target]]
_OUT_FILES = Optional[Iterable[_AnyPath]]
T = TypeVar("T")


def _artefact(
    db: Redis[bytes], store: StorageEngine, data: Union[T, Artefact[T]]
) -> Artefact[T]:
    if isinstance(data, Artefact):
        return data
    else:
        return constant_artefact(db, store, data)


# Yay overloads! we all wish there were an easier way of doing this but here we are...
Tin1 = TypeVar("Tin1", bound=_Data)
Tin2 = TypeVar("Tin2", bound=_Data)
Tin3 = TypeVar("Tin3", bound=_Data)
Tin4 = TypeVar("Tin4", bound=_Data)
Tout1 = TypeVar("Tout1", bound=_Data)
_inp = Union[Tin1, Artefact[Tin1]]


# --------------------------------------------------------------------------------
# Data transformers
def py(  # noqa:C901
    fun: Callable[..., Any],
    *inp: _Target,
    out: Optional[Sequence[Encoding]] = None,
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Connection = None,
) -> Union[Artefact, tuple[Artefact, ...]]:
    """Add a python function to the workflow.

    `py(fun, *inp)` puts a python function `fun` on the workflow and returns
    its output artefact.

    As many arguments will be passed to `fun()` as there are input
    `types.Artefact` instances in `*inp` and `fun()` should return as many
    outputs as there are data types in `out=`. By default, `out=` will be
    inferred from annotations.

    If `strict=False`, the function is taken to do it's own error handling and
    arguments will be of type `errors.Result[T]` instead of `T`. See
    `utils.match_results()` for a convenient way to process these values.

    Python function hashes are generated based on their names (as given by
    `fun.__qualname__`) and functions are distributed to workers using
    `cloudpickle`. This is important because it means that:

    - Workers must have access to the function if it is imported, and must
        have access to any imported libraries.

    - Changing a function without modifiying its name (or modifying the
        `name=` argument) will not recompute the graph.

    It is the therefore the caller's responsibility to `reset()` one of the
    return value of `py()` if the function is modified to ensure re-excution
    of its dependents.

    Args:
        fun: Python function that operates on input artefacts and produces a
            single output artefact.
        *inp: Input artefacts.
        out: List of Encoding, one for each output of fun. These are the kind
            of serialization-deserialization used for the output variables. If
            None, `out=` is inferred using the type hint of `fun()`. It is
            `types.Encoding.blob` for all `bytes` outputs and
            `types.Encoding.json` for anything else.
        name: Override the name of `fun()` used in hash generation.
        strict: If `False`, error handling will be deferred to `fun()` by
            passing it argument of type `errors.Result[bytes]` instead of
            `bytes`.
        connection: An explicit Redis connection. Not required if called
            within a `Fun()` context.
        opt: An `types.Options` instance generated from `options()`. Not
            required if called within a `Fun()` context.

    Returns:
        A single `types.Artefact` instance if `out=` contains only one element
        or a tuple of `types.Artefact` otherwise.

    Raises:
        TypeError:
            The output types could not be determined and were not given.

    """
    # Attempt to infer output
    if out is None:
        out = output_types(fun)

    opt = get_options(opt)
    db, store = get_connection(connection)
    inputs = {}
    for k, arg in enumerate(inp):
        inputs[f"in{k}"] = _artefact(db, store, arg)

    in_types = dict([(k, val.kind) for k, val in inputs.items()])

    noutputs = len(out)
    out_type = dict([(f"out{k}", out[k]) for k in range(noutputs)])
    out_keys = list(out_type.keys())
    in_keys = list(in_types.keys())

    if name is not None:
        fun_name = name
    else:
        fun_name = f"mapping_{len(inp)}:{fun.__qualname__}"

    def __map(inpd: Mapping[str, _Data]) -> dict[str, _Data]:
        """Perform a reduction."""
        args = [inpd[key] for key in in_keys]
        out = fun(*args)
        if noutputs == 1:
            out = (out,)
        return dict(zip(out_keys, out))

    funsie = python_funsie(__map, in_types, out_type, name=fun_name, strict=strict)
    operation = make_op(db, funsie, inputs, opt)
    returnval = tuple(
        [Artefact.grab(db, operation.out[o]) for o in out_keys]  # type:ignore
    )
    if len(returnval) == 1:
        return returnval[0]
    else:
        return returnval


# --------------------------------------------------------------------------------
# Convenience functions


def morph(
    fun: Callable[[Tin1], Tout1],
    inp: Union[Tin1, Artefact[Tin1]],
    *,  # noqa:DAR101,DAR201
    out: Optional[Encoding] = None,
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Connection = None,
) -> Artefact[Tout1]:
    """Add to workflow a one-to-one python function `y = f(x)`.

    This is syntactic sugar around `py()`. By default, the output type will
    match the input type if it can't be inferred, but it can be set to a given
    `types.Encoding` using the `out=` keyword.
    """
    db, store = get_connection(connection)
    inp2 = _artefact(db, store, inp)
    if out is None:
        try:
            typ = output_types(fun)
        except TypeError:
            typ = (inp2.kind,)

        if len(typ) > 1:
            raise TypeError(
                "Attempted to use morph but the function has more than one output.\n"
                + f"inferred return value: {typ}"
            )
        else:
            out = typ[0]

    if name is not None:
        morpher_name = name
    else:
        morpher_name = f"morph:{fun.__qualname__}"
    out_type = [out]
    return py(
        fun,
        inp2,
        out=out_type,
        name=morpher_name,
        strict=strict,
        opt=opt,
        connection=(db, store),
    )


def reduce(
    fun: Callable[..., Tout1],
    *inp: _Target,  # noqa:DAR101,DAR201
    out: Optional[Encoding] = None,
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Connection = None,
) -> Artefact[Tout1]:
    """Add to workflow a many-to-one python function `y = f(*x)`.

    This is syntactic sugar around `py()`. By default, the output encoding is
    inferred, and if this fails, is set to match the encoding of the
    arguments if they are all the same. Output encoding can also be
    explicitly set to a given `types.Encoding` using the `out=` keyword.

    """
    inps = list(inp)
    db, store = get_connection(connection)
    inps2 = [_artefact(db, store, inp) for inp in inps]
    if out is None:
        try:
            typ = output_types(fun)
        except TypeError:
            typ = tuple(set(el.kind for el in inps2))
            if len(typ) > 1:
                raise TypeError(
                    "Inference failed for function reduce(): more than one input type was"
                    + " passed but no out= encoding.\n"
                    + "Either explicitly set return with out= or ensures all inputs "
                    + "have the same encoding.\n"
                    + f"args: {list(el.kind for el in inps2)}\n"
                    + f"inferred possible return values: {typ}"
                )

        if len(typ) > 1:
            raise TypeError(
                "Attempted to use reduce but the function has more than one output.\n"
                + f"inferred return value: {typ}"
            )
        else:
            out = typ[0]

    if name is not None:
        morpher_name = name
    else:
        morpher_name = f"reduce:{fun.__qualname__}"
    out_type = [out]
    return py(
        fun,
        *inps2,
        out=out_type,
        name=morpher_name,
        strict=strict,
        opt=opt,
        connection=(db, store),
    )
