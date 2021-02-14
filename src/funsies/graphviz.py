"""graphviz related utilities."""
from __future__ import annotations

# std
import os.path

# external
from redis import Redis
import rq
from rq.queue import Queue
from msgpack import unpackb

# module
from ._funsies import get_funsie, FunsieHow
from ._graph import get_artefact, get_op, get_op_options, get_status
from ._short_hash import shorten_hash
from .constants import DAG_CHILDREN, DAG_INDEX, DAG_PARENTS, DAG_STORE, hash_t
from .logging import logger
from .run import run_op, RunStatus
import itertools


def export(db: Redis[bytes], address: hash_t) -> str:
    """Output a DAG in dot format for graphviz."""
    if (DAG_STORE + address).encode() not in db.smembers(DAG_INDEX):
        logger.error(
            f"attempted to print dag for {address}, but it has not been generated"
        )
        return ""

    nodes = {}
    artefacts = {}

    # add node data
    for element in db.smembers(DAG_STORE + address):
        # all the operations
        h = hash_t(element.decode())
        nodes[h] = {}
        obj = get_op(db, h)
        funsie = get_funsie(db, obj.funsie)
        if funsie.how == FunsieHow.shell:
            nodes[h]["label"] = ";".join(unpackb(funsie.what)["cmds"])
        else:
            nodes[h]["label"] = funsie.what.decode()

        nodes[h]["inputs"] = obj.inp
        nodes[h]["outputs"] = obj.out

        for k in itertools.chain(obj.inp.values(), obj.out.values()):
            artefacts[k] = get_status(db, k)

    return nodes, artefacts


def colors(i):
    if i == 1:
        return "green"
    elif i == 0:
        return "gray"
    elif i == 2:
        return "blue"
    elif i == 3:
        return "red"
    else:
        return "white"


def sanitize_command(lab):
    return lab.replace("<", "\<").replace(">", "\>")


def sanitize_fn(n):
    return os.path.basename(n)


def gvdraw(nodes, artefacts, targets):
    # Connections
    keep = {}
    finals = {}
    initials = {}
    for n in nodes:
        for k, v in nodes[n]["inputs"].items():
            keep[v] = keep.get(v, []) + [n]
            if artefacts[v] == 2:
                initials[v] = initials.get(v, []) + [n]

        for t in targets:
            if t in nodes[n]["outputs"].values():
                keep[t] = []
                finals[t] = n

    nstring = ""
    for n in nodes:
        inps = []
        for k, v in nodes[n]["inputs"].items():
            if v in keep:
                inps += [f"<A{v}>{sanitize_fn(k)}"]
        inps = "|".join(inps)

        outs = []
        for k, v in nodes[n]["outputs"].items():
            if v in keep:
                outs += [f"<A{v}>{sanitize_fn(k)}"]
        outs = "|".join(outs)

        nstring += (
            f"N{n} ["
            + "shape=record,width=.1,height=.1,"
            + 'label="'
            + "{{"
            + f"{inps}"
            + "}|"
            + f"{n[:6]} \\n {sanitize_command(nodes[n]['label'])}"
            + "|{"
            + f"{outs}"
            + "}}"
            + '"];\n'
        )

    # Initial data
    for k in initials:
        nstring += f'I{k} [label="{k[:6]}"];\n'

    for k in finals:
        nstring += f'F{k} [label="{k[:6]}"];\n'

    connect = ""
    for n in nodes:
        for k, v in nodes[n]["outputs"].items():
            for n2 in keep.get(v, []):
                connect += (
                    f"N{n}:A{v} -> N{n2}:A{v} "
                    + f'[label="{v[:6]}",'
                    + f" color={colors(artefacts[v])}];\n"
                )

    for k, values in initials.items():
        for v in values:
            connect += f"I{k} -> N{v}:A{k};\n"

    for k, v in finals.items():
        connect += f"N{v}:A{k} -> F{k};\n"

    header = "digraph G {\nrankdir=LR;\n"
    footer = "\n}"
    return header + nstring + connect + footer
