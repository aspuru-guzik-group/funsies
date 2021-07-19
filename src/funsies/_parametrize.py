"""Parametrize DAGs and re-run them dynamically."""
from __future__ import annotations

# std
from dataclasses import dataclass
import hashlib
from typing import cast, Optional, Set, Type

# external
from redis import Redis
from redis.client import Pipeline

# module
from ._constants import ARTEFACTS, hash_t, join, OPERATIONS, PARAMETRIC
from ._dag import ancestors, descendants
from ._funsies import Funsie
from ._graph import Artefact, make_op, Operation
from ._logging import logger


def _parametrize_subgraph(
    db: Redis[bytes], inputs: dict[str, Artefact], outputs: dict[str, Artefact]
) -> set[hash_t]:
    """Return the subgraph of operators that connects given inputs and outputs."""
    # First, we grab all the ancestors for all the outputs.
    output_ops = set()
    for o in outputs.values():
        if o.parent == "root":
            raise RuntimeError("output of parametrization has no dependencies.")
        else:
            output_ops.add(o.parent)
    out_ancestors = ancestors(db, *output_ops).union(output_ops)

    # then we grab all the descendants for all the inputs.
    input_ops = set()
    for name, inp in inputs.items():
        inp_dependent_ops = []
        for dependent in db.smembers(join(ARTEFACTS, inp.hash, "dependents")):
            d: str = dependent.decode()
            if d in out_ancestors:
                inp_dependent_ops += [d]

        if len(inp_dependent_ops) == 0:
            logger.error(f"parametrized input {name} does not change any outputs!")

        input_ops.update(inp_dependent_ops)
    input_ops = cast(Set[hash_t], input_ops)
    in_descendants = descendants(db, *input_ops).union(input_ops)

    # The intersection between these two sets forms the set of operators that
    # we need in our parametrization.
    return out_ancestors.intersection(in_descendants)


def _subgraph_edges(db: Redis[bytes], nodes: set[hash_t]) -> dict[hash_t, set[hash_t]]:
    """Compute edges on a connected subgraph."""
    # get edges
    edges: dict[hash_t, set[hash_t]] = {}
    for n in nodes:
        for value in db.smembers(join(OPERATIONS, n, "parents")):
            parent: hash_t = hash_t(value.decode())
            if parent in nodes:
                edges[parent] = edges.get(parent, set()).union([n])
    return edges


def _subgraph_toposort(
    nodes: set[hash_t], edges: dict[hash_t, set[hash_t]]
) -> list[hash_t]:
    """Sort a connected subgraph topologically."""
    # from https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm

    # remove all the nodes that have dependencies to get the initial set
    S = nodes.copy()
    for out in edges.values():
        for k in out:
            S.discard(k)

    output = []
    while len(S):
        node_n = S.pop()
        output.append(node_n)
        edge_set = edges.pop(node_n, set())

        # keep only the nodes with no dependencies
        for out in edges.values():
            for k in out:
                edge_set.discard(k)

        S = S.union(edge_set)
    return output


def _hash_parametric(
    db: Redis[bytes],
    sorted_nodes: list[hash_t],
    inputs: dict[str, Artefact],
    outputs: dict[str, Artefact],
) -> hash_t:
    """Get hash for a Parametric."""
    # This is done basically the exact same way as evaluating a parametric,
    # but hashing instead of evaluating.

    translation: dict[hash_t, hash_t] = {}  # old hash -> new artefact
    # bootstart using the new input values
    for name in inputs.keys():
        translation[inputs[name].hash] = hash_t(name)

    # these are sorted so this is fairly straightforward
    for node in sorted_nodes:
        op = Operation.grab(db, node)
        new_inp = {}
        for k, v in op.inp.items():
            if v not in translation:
                translation[v] = v
                # TODO: we could do artefact freezing here relatively easily.
            new_inp[k] = translation[v]

        # ==============================================================
        #     ALERT: DO NOT TOUCH THIS CODE WITHOUT CAREFUL THOUGHT
        # --------------------------------------------------------------
        # When hashes change, previous databases become deprecated. This
        # (will) require a change in version number!
        m = hashlib.sha1()
        m.update(op.funsie.encode())
        for key, val in sorted(new_inp.items(), key=lambda x: x[0]):
            m.update(f"file={key}, hash={val}".encode())
        ophash = hash_t(m.hexdigest())

        for key, val in op.out.items():
            translation[val] = hash_t(ophash + ":" + key)

    # STILL NO TOUCHING
    m = hashlib.sha1()
    m.update("parametric".encode())
    for key, art in sorted(outputs.items(), key=lambda x: x[0]):
        m.update(f"output:{key}, hash:{translation[art.hash]}".encode())

    return hash_t(m.hexdigest())


def _do_parametrize(
    db: Redis[bytes],
    sorted_nodes: list[hash_t],
    inputs: dict[str, hash_t],
    outputs: dict[str, hash_t],
    new_inputs: dict[str, Artefact],
) -> dict[str, Artefact]:
    """Take a subgraph and re-build it with input values changed for new_inputs."""
    translation = {}  # old hash -> new artefact
    # bootstart using the new input values
    for name, artefact in new_inputs.items():
        translation[inputs[name]] = artefact

    # these are sorted so this is fairly straightforward
    for node in sorted_nodes:
        op = Operation.grab(db, node)
        new_inp = {}
        for k, v in op.inp.items():
            if v not in translation:
                # update to minimize the amount of artefact grabbing
                translation[v] = Artefact.grab(db, v)
                # TODO: we could do artefact freezing here relatively easily.

            new_inp[k] = translation[v]

        funsie = Funsie.grab(db, op.funsie)
        assert op.options is not None  # not needed as it is never None
        new_op = make_op(db, funsie, new_inp, op.options)
        for k, v in new_op.out.items():
            # update translation map with new ops artefacts
            translation[op.out[k]] = Artefact.grab(db, v)

    return_value = {}
    for k, h in outputs.items():
        return_value[k] = translation[h]
    return return_value


@dataclass(frozen=True)
class Parametric:
    """Parametrized DAG."""

    name: str
    hash: hash_t
    ops: list[hash_t]
    inp: dict[str, hash_t]
    out: dict[str, hash_t]

    def evaluate(
        self: Parametric, db: Redis[bytes], new_inp: dict[str, Artefact]
    ) -> dict[str, Artefact]:
        """Generate DAG from a Parametric."""
        return _do_parametrize(db, self.ops, self.inp, self.out, new_inp)

    def put(self: Parametric, db: Redis[bytes]) -> None:
        """Save a Parametric DAG to Redis."""
        db.hset(join(PARAMETRIC, self.hash, "inp"), mapping=self.inp)  # type:ignore
        db.hset(join(PARAMETRIC, self.hash, "out"), mapping=self.out)  # type:ignore
        db.rpush(join(PARAMETRIC, self.hash), *self.ops)
        db.set(join(PARAMETRIC, self.hash, "name"), self.name.encode())

        # set the name for reverse lookups
        db.hset(
            join(PARAMETRIC, hash_t("names")), self.name.encode(), self.hash.encode()
        )

    @classmethod
    def grab(cls: Type[Parametric], db: Redis[bytes], hash: hash_t) -> Parametric:
        """Grab a Parametric DAG from the Redis store."""
        pipe: Pipeline = db.pipeline(transaction=False)
        pipe.exists(join(PARAMETRIC, hash))
        pipe.lrange(join(PARAMETRIC, hash), 0, -1)
        pipe.hgetall(join(PARAMETRIC, hash, "inp"))
        pipe.hgetall(join(PARAMETRIC, hash, "out"))
        pipe.get(join(PARAMETRIC, hash, "name"))
        exists, lrange, inp, out, name = pipe.execute()

        if not exists:
            raise RuntimeError(f"No parametric DAG at {hash}")

        name = name.decode()
        ops = [hash_t(el.decode()) for el in lrange]

        return Parametric(
            name=name,
            hash=hash,
            ops=ops,
            inp=dict([(k.decode(), hash_t(v.decode())) for k, v in inp.items()]),
            out=dict([(k.decode(), hash_t(v.decode())) for k, v in out.items()]),
        )

    @classmethod
    def resolve_name(
        cls: Type[Parametric], db: Redis[bytes], name: str
    ) -> Optional[hash_t]:
        """Get the parametric hash corresponding to a given name."""
        h = db.hget(join(PARAMETRIC, hash_t("names")), name)
        if h is None:
            return None
        else:
            return hash_t(h.decode())


def make_parametric(
    db: Redis[bytes],
    name: str,
    inp: dict[str, Artefact],
    out: dict[str, Artefact],
) -> Parametric:
    """Define a parametric DAG."""
    # Get the ops
    ops = _parametrize_subgraph(db, inp, out)

    # Sort topologically
    edges = _subgraph_edges(db, ops)
    sorted_ops = _subgraph_toposort(ops, edges)

    param = Parametric(
        name=name,
        hash=_hash_parametric(db, sorted_ops, inp, out),
        ops=sorted_ops,
        inp=dict([(k, v.hash) for k, v in inp.items()]),
        out=dict([(k, v.hash) for k, v in out.items()]),
    )

    # Save in db
    pipe: Pipeline = db.pipeline(transaction=False)
    param.put(pipe)
    pipe.execute()
    return param
