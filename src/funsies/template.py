"""Contains `template()`, a function to create templated scripts."""
from __future__ import annotations

# std
import os
from typing import Any, Mapping, Optional, TypeVar, Union

# external
import chevron
from redis import Redis

# module
from ._constants import Encoding
from ._context import Connection, get_connection, get_options
from ._graph import Artefact, constant_artefact, make_op
from ._pyfunc import python_funsie
from ._storage import StorageEngine
from .config import Options

T = TypeVar("T")


def _artefact(
    db: Redis[bytes], store: StorageEngine, data: Union[T, Artefact[T]]
) -> Artefact[T]:
    if isinstance(data, Artefact):
        return data
    else:
        return constant_artefact(db, store, data)  # type:ignore


_Template = Union[str, Artefact[str], bytes, Artefact[bytes]]
_Value = Any


def template(
    template: _Template,
    data: Mapping[str, _Value],
    strip: bool = True,
    *,
    env: Optional[Mapping[str, str]] = None,
    name: Optional[str] = None,
    opt: Optional[Options] = None,
    connection: Connection = None,
) -> Artefact[bytes]:
    """Fill in a template using data from artefacts.

    This function takes a [chevron template](https://mustache.github.io/) and
    fills it with the dictionary ``data``,

    ```python
    funsies = f.template(template, data)
    # corresponds basically to running
    normal = chevron.render(template, data)
    ```

    ``template()`` is a full-featured funsies function: both the template and
    the data can come from the database and are (as usual) lazily evaluated.
    Substitutions provided in the ``env=`` dictionary are expanded using
    environment variables.

    The primary intended use of ``template()`` is the generation of input
    files for simulation software. For example,

    ```python
    # funsies
    import funsies as f

    g16_template = \"""%NProcShared={{nthreads}}
    # {{functional}}/{{basis}} Symm=None {{type}}

    Gaussian calculation

    {{charge}} {{spin}}
    {{structure}}

    \"""

    with f.Fun():
        inp = f.template(
            g16_template,
            {
                "functional": "b3lyp",
                "basis": "6-31g",
                "type": "sp",
                "spin": 1,
                # the next two could be obtained eg from conformer generation.
                "charge": charge,
                "structure": coords,
            },
            env={"nthreads": "OMP_NUM_THREADS"},
        )
        dft_job = f.shell(
            "g16 input.com", inp={"input.com": inp}, out=["input.log", "data.chk"]
        )
    ```

    Args:
        template: The template, either as a string or as an ``types.Artefact[str]``.
        data: A ``dict[key, value]`` of substitutions to perform on the
            template. ``value`` can be any type accepted by ``chevrons``
            (``str`` but also ``int``, ``bytes`` etc.) and/or ``types.Artefact``
            objects containing those types.
        strip: If `True`, substitutions will be ``.strip()`` before templating.
        env: A ``dict[str,str]`` of substitutions to fill in from the
            environment variables of the worker process.
        name: Provide an explicit name to the template.
        connection: An explicit Redis connection. Not required if called within a
            `Fun()` context.
        opt: An `types.Options` instance as returned by `options()`. Not
            required if called within a `Fun()` context.

    Returns:
        An `types.Artefact[bytes]` object, populated with the generated
        template as a bytestring.
    """  # noqa:D300,D301
    # Get context elements
    opt = get_options(opt)
    db, store = get_connection(connection)

    # Make sure template is a string in the db
    if isinstance(template, bytes):
        tmp = _artefact(db, store, template.decode())
    else:
        tmp = _artefact(db, store, template)  # type:ignore

    # Make sure all substitutions are strings in the db
    args = dict()
    for key, value in data.items():
        if isinstance(value, bytes):
            value = value.decode()
        args[key] = _artefact(db, store, value)

    template_key = "template"
    while template_key in args:
        template_key += "_"  # append more _ until template_key is unique

    env_key = "env"
    while env_key in args:
        env_key += "_"  # append more _ until template_key is unique

    args[template_key] = tmp
    args[env_key] = _artefact(db, store, env)

    in_types = dict([(k, val.kind) for k, val in args.items()])

    if name is not None:
        fun_name = name
    else:
        do_strip = ""
        if strip:
            do_strip = ", stripped"
        fun_name = f"template (chevrons{do_strip})"

    def __exec(inpd: Mapping[str, Any]) -> dict[str, bytes]:
        """Substitute into template."""
        args = {}
        for key, val in inpd.items():
            if isinstance(val, bytes):
                val = val.decode()
            if isinstance(val, str) and strip and key != template_key:
                val = val.strip()
            args[key] = val

        # read template
        template = args[template_key]

        # read env variables
        if args[env_key] is not None:
            env = args[env_key]
            for key, val in env.items():
                args[key] = os.environ.get(val)

        del args[template_key]
        del args[env_key]

        return {"out": chevron.render(template, args).encode()}

    funsie = python_funsie(
        __exec, in_types, {"out": Encoding.blob}, name=fun_name, strict=True
    )
    operation = make_op(db, funsie, args, opt)
    return Artefact.grab(db, operation.out["out"])
