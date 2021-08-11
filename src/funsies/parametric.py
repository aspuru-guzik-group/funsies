"""User-facing functions for parametric DAGs."""
from __future__ import annotations

# std
from typing import Any, Optional

# external
from redis import Redis

# module
from ._constants import hash_t
from ._context import get_redis
from ._graph import Artefact
from ._parametrize import make_parametric, Parametric
from .ui import _Target, put


def commit(
    name: str,
    inp: dict[str, Artefact[Any]],
    out: dict[str, Artefact[Any]],
    *,
    connection: Optional[Redis[bytes]] = None,
) -> hash_t:
    """Parametrize and commit a workflow.

    This function gives a name to part of a workflow and parametrizes it for
    use with `parametric.recall`. This is probably best described by an
    example,

    ```
    import funsies as f

    name_john = f.put("john")
    name_John = f.morph(lambda x: x.capitalize(), name)
    f.parametric.commit(
        "capitalize",
        in={"name":name_john},
        out={"name_out":name_John}
    )
    ```

    Now say we wanted to capitalize other names, we can recall the
    `"capitalize"` workflow with new input parameters,

    ```
    out = f.parametric.recall("capitalize", {"name":"tim"})
    f.execute(out["name_out"])
    f.take(out["name_out"])     # returns "Tim"
    ```

    These parametric workflows can be used across scripts, machines, etc. They
    allow encoding steps that can be reproduced without access to the original
    workflow script.

    The graph of task from `inp` to `out` artefacts (and any depedencies) is
    what is saved by `parametric.commit()`.

    Specifically, the `inp` and `out` artefacts defines the workflow to
    encode. Only those input artefacts that will need to be modified on
    `parametric.recall()` need to be explicitly passed. Only those output
    artefacts that we would like computed needs to be passed in `out`. Any
    other necessary inputs will be loaded from the artefact store but not
    parametrized, that is, with the values they had at the time of
    `parametric.commit()`.

    Args:
        name: Name of the parametric DAG.
        inp: Dictionary of artefacts that form the inputs to the Parametric DAG.
        out: Dictionary of artefacts that form the outputs to the Parametric
            DAG. `inp` and `out` together define the committed DAG.
        connection: An explicit Redis connection. Not required if called within a
            `Fun()` context.

    Returns:
        The hash value of the committed Parametric DAG.
    """
    db = get_redis(connection)
    param = make_parametric(db, name, inp, out)
    return param.hash


def recall(
    name_or_hash: str,
    inp: dict[str, _Target],
    *,
    connection: Optional[Redis[bytes]] = None,
) -> dict[str, Artefact[Any]]:
    """Recall a Parametric DAG and evaluate it.

    This function recalls a workflow previously encoded with
    `parametric.commit()`, substitutes in the values in `inp` and returns a
    dictionary of artefacts (from `out` in `parametric.commit()`) that
    descends from the new input values.

    Note that every step is automatically duplicated to account for new data
    if and only if doing so is necessary. That is, the calculation is
    incremental.

    Args:
        name_or_hash: Name or hash value of a DAG encoded with `parametric.commit()`.
        inp: Dictionary of artefacts corresponding to those passed to
            `parametric.commit()`. Any omitted data will simply be substituted by
            whatever was committed.
        connection: An explicit Redis connection. Not required if called within a
            `Fun()` context.

    Returns:
        A dictionary of artefacts that corresponds to the `out=` argument of
        `parametric.commit()`.
    """
    db = get_redis(connection)

    # first, check if its a name
    h = Parametric.resolve_name(db, name_or_hash)
    if h is None:
        # Probably a hash then
        h = hash_t(name_or_hash)

    param = Parametric.grab(db, h)
    new_inps = {}
    for k, arg in inp.items():
        if k not in param.inp:
            raise AttributeError(
                f"input {k} to parametric {h} not in defined inputs"
                + f" {list(param.inp.keys())}"
            )
        if isinstance(arg, Artefact):
            new_inps[k] = arg
        else:
            new_inps[k] = put(arg, connection=db)

    # TODO: encoding check?
    # for k, art in new_inps.items():
    #     if art.kind != param.inp[k].kind:
    #         raise TypeError(
    #             f"input {k} to parametric {h} as encoding {art.kind},"
    #             + f" expected {param.inp[k].kind}"
    #         )

    return param.evaluate(db, new_inps)
