"""graphviz related utilities."""
from __future__ import annotations

# std
import itertools
import os.path
from typing import cast, Dict

# external
from msgpack import unpackb
from redis import Redis

# module
from ._funsies import FunsieHow, get_funsie
from ._graph import ArtefactStatus, get_op, get_status
from ._short_hash import shorten_hash
from .constants import DAG_INDEX, DAG_STORE, hash_t
from .dag import build_dag
from .logging import logger

__node_type = Dict[hash_t, Dict[str, Dict[str, hash_t]]]
__artefact_type = Dict[str, ArtefactStatus]
__label_type = Dict[str, str]


def __sanitize_command(lab: str) -> str:
    if ".<locals>" in lab:
        lab = lab.split(".<locals>")[0]
    return lab.replace("<", r"\<").replace(">", r"\>")


def export(
    db: Redis[bytes], address: hash_t
) -> tuple[__node_type, __artefact_type, __label_type]:
    """Output a DAG in dot format for graphviz."""
    if (DAG_STORE + address).encode() not in db.smembers(DAG_INDEX):
        logger.warning(
            f"attempted to print dag for {address}, but "
            + "it has not been generated. building now."
        )
        build_dag(db, address)

    nodes: __node_type = {}
    labels: dict[str, str] = {}
    artefacts: __artefact_type = {}

    # add node data
    for element in db.smembers(DAG_STORE + address):
        element = cast(bytes, element)
        # all the operations
        h = hash_t(element.decode())
        nodes[h] = {}
        obj = get_op(db, h)
        funsie = get_funsie(db, obj.funsie)

        if funsie.how == FunsieHow.shell:
            labels[h] = __sanitize_command(";".join(unpackb(funsie.what)["cmds"]))
        else:
            labels[h] = __sanitize_command(funsie.what.decode())

        if funsie.error_tolerant:
            labels[h] += r"\n(tolerates err)"

        nodes[h]["inputs"] = obj.inp
        nodes[h]["outputs"] = obj.out

        for k in itertools.chain(obj.inp.values(), obj.out.values()):
            artefacts[k] = get_status(db, k)

    return nodes, artefacts, labels


def __style_line(i: ArtefactStatus) -> str:
    if i == 1:
        return "color=2,penwidth=3.0"
    elif i == 0:
        return "color=8,penwidth=3.0"
    elif i == 2:
        return "color=black,penwidth=1.0"
    elif i == 3:
        return "color=6,penwidth=3.0"
    else:
        return "color=black"


def __style_node(i: ArtefactStatus) -> str:
    if i == 1:
        return "style=filled,color=1"
    if i == 0:
        return "style=filled,color=7"
    if i == 2:
        return "color=white"
    elif i == 3:
        return "style=filled,color=5"
    else:
        return ""


def __sanitize_fn(n: str) -> str:
    return os.path.basename(n)


def format_dot(  # noqa:C901
    nodes: __node_type,
    artefacts: __artefact_type,
    labels: __label_type,
    targets: list[hash_t],
) -> str:
    """Format output of export() for graphviz dot."""
    header = (
        "digraph G {\nrankdir=LR;\n"
        + "node [fontsize=24, colorscheme=paired8];\n"
        + "edge [fontsize=24, colorscheme=paired8];\n"
    )

    keep: dict[hash_t, list[hash_t]] = {}
    finals: dict[hash_t, hash_t] = {}
    initials: dict[hash_t, list[hash_t]] = {}
    # Setup which artefacts to show, which are inputs and which are outputs.
    for n in nodes:
        for v in nodes[n]["inputs"].values():
            keep[v] = keep.get(v, []) + [n]
            if artefacts[v] == 2:
                initials[v] = initials.get(v, []) + [n]

        for t in targets:
            if t in nodes[n]["outputs"].values():
                keep[t] = []
                finals[t] = n

    # write operation nodes
    nstring = ""
    for n in nodes:
        inps = []
        for k, v in nodes[n]["inputs"].items():
            if v in keep:
                inps += [f"<A{v}>{__sanitize_fn(k)}"]
        inps = "|".join(inps)

        outs = []
        for k, v in nodes[n]["outputs"].items():
            if v in keep:
                outs += [f"<A{v}>{__sanitize_fn(k)}"]
        outs = "|".join(outs)

        nstring += (
            f"N{n} ["
            + "shape=record,width=.1,height=.1,"
            + 'label="'
            + "{{"
            + f"{inps}"
            + "}|"
            + f" {shorten_hash(n)} \\n{labels[n]}"
            + "|{"
            + f"{outs}"
            + "}}"
            + '"];\n'
        )

    # write artefact nodes
    for k in keep:
        nstring += (
            f"A{k}"
            + f'[shape=box,label="{shorten_hash(k)}"'
            + f",{__style_node(artefacts[k])}];\n"
        )

    # Make connections
    connect = ""
    for n in nodes:
        for _, v in nodes[n]["outputs"].items():
            if v in keep:
                connect += f"N{n}:A{v} -> A{v} [{__style_line(artefacts[v])}];\n"

        for _, v in nodes[n]["inputs"].items():
            if v in keep:
                connect += f"A{v} -> N{n}:A{v} [{__style_line(artefacts[v])}];\n"

    init = []
    for a in artefacts:
        if artefacts[a] == 2:
            init += [f"A{a}"]
    ranks = "{rank = same;" + ";".join(init) + ";}\n"

    final = []
    for t in targets:
        if t in artefacts:
            final += [f"A{t}"]
        else:
            final += [f"N{t}"]
    ranks += "{rank = same;" + ";".join(final) + ";}\n"

    footer = "\n}"
    return header + nstring + connect + ranks + footer
