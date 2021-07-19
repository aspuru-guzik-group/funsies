"""graphviz related utilities."""
from __future__ import annotations

# std
import itertools
import os.path
from typing import Dict, Optional

# external
from redis import Redis

# module
from ._constants import ARTEFACTS, FUNSIES, hash_t, join
from ._dag import ancestors, get_nearest_operation
from ._graph import ArtefactStatus, Operation, resolve_link
from ._logging import logger
from ._short_hash import shorten_hash

__node_type = Dict[hash_t, Dict[str, Dict[str, hash_t]]]
__artefact_type = Dict[hash_t, ArtefactStatus]
__label_type = Dict[str, str]
__link_type = Dict[hash_t, hash_t]

# Watch out: nasty code ahead. This functionality is quite secondary to the
# program, so it's not written with the most care. Likely, the following will
# basically need a full rewrite eventually. If it breaks... maybe we just
# chuck it.


def __sanitize_command(lab: str) -> str:
    if ".<locals>" in lab:
        lab = lab.split(".<locals>")[0]
    return (
        lab.replace("<", r"\<")
        .replace(">", r"\>")
        .replace("|", r"\|")
        .replace("{", r"\{")
        .replace("}", r"\}")
        .replace('"', r"\"")
        .replace("&&", r"\n")
    )


def export(
    db: Redis[bytes], addresses: list[hash_t]
) -> tuple[__node_type, __artefact_type, __label_type, __link_type]:
    """Output a DAG in dot format for graphviz."""
    nodes: __node_type = {}
    labels: dict[str, str] = {}
    artefacts: __artefact_type = {}

    funsies = {}
    art_hashes = set()

    for address in addresses:
        node = get_nearest_operation(db, address)
        if node is None:
            continue

        curr_nodes = ancestors(db, node.hash, include_subdags=True)
        curr_nodes.add(node.hash)
        logger.info(f"graph contains {len(curr_nodes)} nodes")

        for k, h in enumerate(curr_nodes):
            if k % 100 == 0 and k > 0:
                logger.info(f"done: {k} / {len(curr_nodes)}")
            # all the operations
            nodes[h] = {}
            obj = Operation.grab(db, h)

            # get funsies data, cache it too
            if obj.funsie not in funsies:
                funsies[obj.funsie] = db.hgetall(join(FUNSIES, obj.funsie))
            funsie = funsies[obj.funsie]

            labels[h] = __sanitize_command(funsie[b"what"].decode())
            if funsie[b"error_tolerant"] == b"1":
                labels[h] += r"\n(tolerates err)"

            nodes[h]["inputs"] = obj.inp
            nodes[h]["outputs"] = obj.out

            for h in itertools.chain(obj.inp.values(), obj.out.values()):
                # cache status too
                art_hashes.add(h)

    logger.info(f"gathering status for {len(art_hashes)} artefacts")
    artefact_hashes = list(art_hashes)
    redis_keys = [join(ARTEFACTS, address, "status") for address in artefact_hashes]
    statuses: list[Optional[bytes]] = db.mget(redis_keys)
    for h, s in zip(artefact_hashes, statuses):
        if s is None:
            artefacts[h] = ArtefactStatus.not_found
        else:
            artefacts[h] = ArtefactStatus(int(s.decode()))

    # Get links
    links: dict[hash_t, hash_t] = {}
    for h, v in artefacts.items():
        if v == ArtefactStatus.linked:
            links[h] = resolve_link(db, h)
    logger.info(f"found {len(links)} links")

    return nodes, artefacts, labels, links


__style_line_link = "color=black,penwidth=8.0"


def __style_line(i: ArtefactStatus) -> str:
    if i == 1:
        return "color=2,penwidth=3.0"
    elif i <= 0:
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
    if i == -2:
        return "style=filled,color=7,fontcolor=6"
    if i == 2:
        return "color=white"
    elif i == 3:
        return "style=filled,color=5"
    elif i == 4:
        return "color=white"
    else:
        return ""


def __sanitize_fn(n: str) -> str:
    return os.path.basename(n)


def format_dot(  # noqa:C901
    nodes: __node_type,
    artefacts: __artefact_type,
    labels: __label_type,
    links: dict[hash_t, hash_t],
    targets: list[hash_t],
    show_inputs: bool = True,
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
            # todo make conditional keep setable
            if artefacts[v] == 2:
                initials[v] = initials.get(v, []) + [n]
                if show_inputs:
                    keep[v] = keep.get(v, []) + [n]
            else:
                keep[v] = keep.get(v, []) + [n]

        # if targets are artefacts, then we should always keep them
        for t in targets:
            if t in nodes[n]["outputs"].values():
                keep[t] = []
                finals[t] = n

        # if targets are nodes, then we should always keep all the artefacts.
        if n in targets:
            for v in nodes[n]["outputs"].values():
                keep[v] = keep.get(v, []) + [n]
                finals[v] = n

    # keep all the links too
    for k, v in links.items():
        keep[v] = keep.get(v, []) + [k]

    # write operation nodes
    nstring = ""
    for n in nodes:
        inps = []
        for kk, v in nodes[n]["inputs"].items():
            if v in keep:
                inps += [f"<A{v}>{__sanitize_fn(kk)}"]
        inps = "|".join(inps)

        outs = []
        for kk, v in nodes[n]["outputs"].items():
            if v in keep:
                outs += [f"<A{v}>{__sanitize_fn(kk)}"]
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
    for k in set(keep):
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

    ranks = ""
    for k, v in links.items():
        connect += f"A{v} -> A{k} [{__style_line_link}];\n"
        ranks += "{" + f"rank = same; A{v}; A{k};" + "}\n"

    if show_inputs:
        init = []
        for a in initials:
            init += [f"A{a}"]
        if len(init):
            ranks += "{rank = same;" + ";".join(init) + ";}\n"

    # rank_last = []
    # for t in finals.keys():
    #     rank_last += [f"A{t}"]
    # ranks += "{rank = same;" + ";".join(rank_last) + ";}\n"

    footer = "\n}"
    return header + nstring + connect + ranks + footer
