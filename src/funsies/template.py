"""Contains `template()`, a function to create templated scripts."""
from __future__ import annotations

# std
from typing import Any, Mapping, Optional, TypeVar, Union

# external
import chevron
from redis import Redis

# module
from ._constants import Encoding
from ._context import get_db, get_options
from ._graph import Artefact, constant_artefact, make_op
from ._pyfunc import python_funsie
from .config import Options

T = TypeVar("T")


def _artefact(db: Redis[bytes], data: Union[T, Artefact[T]]) -> Artefact[T]:
    if isinstance(data, Artefact):
        return data
    else:
        return constant_artefact(db, data)  # type:ignore


_Template = Union[str, Artefact[str], bytes, Artefact[bytes]]
_Value = Any


def template(
    template: _Template,
    data: Mapping[str, _Value],
    *,
    name: Optional[str] = None,
    strict: bool = True,
    opt: Optional[Options] = None,
    connection: Optional[Redis[bytes]] = None,
) -> Artefact[bytes]:
    """Fill in a template."""
    # Get context elements
    opt = get_options(opt)
    db = get_db(connection)

    # Make sure template is a string in the db
    if isinstance(template, bytes):
        tmp = _artefact(db, template.decode())
    else:
        tmp = _artefact(db, template)  # type:ignore

    # Make sure all substitutions are strings in the db
    args = dict()
    for key, value in data.items():
        if isinstance(value, bytes):
            value = value.decode()
        args[key] = _artefact(db, value)

    template_key = "template"
    while template_key in args:
        template_key += "_"  # append more _ until template_key is unique

    args[template_key] = tmp
    in_types = dict([(k, val.kind) for k, val in args.items()])

    if name is not None:
        fun_name = name
    else:
        fun_name = "template (chevrons)"

    def __exec(inpd: Mapping[str, Any]) -> dict[str, bytes]:
        """Substitute into template."""
        args = {}
        for key, val in inpd.items():
            if isinstance(val, bytes):
                args[key] = val.decode()
            else:
                args[key] = val
        template = args[template_key]
        del args[template_key]
        return {"out": chevron.render(template, args).encode()}

    funsie = python_funsie(
        __exec, in_types, {"out": Encoding.blob}, name=fun_name, strict=strict
    )
    operation = make_op(db, funsie, args, opt)
    return Artefact.grab(db, operation.out["out"])
