"""Infer function types."""
from __future__ import annotations

import inspect
from typing import Any, Callable, Tuple, Type, Union


# python 3.7 imports Literal from typing_extensions
try:
    from typing import get_args, get_origin
except ImportError:
    from typing_extensions import get_args, get_origin

from ._constants import Encoding
from ._logging import logger


def __translate(x: Union[Type[Any], str]) -> Encoding:
    if x is bytes or x == "bytes":
        return Encoding.blob
    else:
        return Encoding.json


# Type a function
def output_types(fun: Callable[..., Any]) -> tuple[Encoding, ...]:
    """Infer number of return values from a function's type hint."""
    sig = inspect.signature(fun)
    returns = sig.return_annotation
    if type(returns) == str:
        raise TypeError(
            "Failed to infer output types: return annotation is absent.\n"
            + f"signature: {sig} name: {fun.__name__}\n"
            + "Return value converted to string.\nAre you using"
            + "from __future__ import annotations on python=3.7?"
        )

    if returns is inspect.Signature.empty:
        if fun.__name__ == "<lambda>":
            logger.debug("warning: lambda function has no return types")

        raise TypeError(
            "Failed to infer output types: return annotation is absent.\n"
            + f"signature: {sig} name: {fun.__name__}\n"
            + "You need to define your outputs explicitly"
        )
    elif returns is None or returns == "None":
        return ()
    else:
        generic = get_origin(returns)
        if (
            generic is tuple
            or generic is Tuple
            or generic == "Tuple"
            or generic == "tuple"
        ):
            args = get_args(returns)
            if Ellipsis in args:
                raise TypeError(
                    "Failed to infer output types: return annotation is too broad.\n"
                    + f"signature: {sig} name: {fun.__name__}\n"
                    + "It's not clear how many elements this function should output."
                    + "You need to define your outputs explicitly"
                )
            return tuple(__translate(t) for t in args)
        else:
            return (__translate(returns),)
