"""Completely unnecessary distributed merge sort."""
from __future__ import annotations

# std
import random
from typing import TypeVar

# funsies
import funsies as f
from funsies.types import Artefact, Encoding, Error, Result

T = TypeVar("T")

# We stop recusion dynamically using an Exception, in a manner similar to
# raising StopIteration in a standard python program. This Exception is caught
# below in __main__ using ignore_error().


class RecursionStop(Exception):
    """Exception raised recursion limit has been reached."""

    pass


def split(inp: list[int]) -> tuple[list[int], list[int]]:
    """Split a list into two sublist, raise if given less than 2 elements."""
    if len(inp) < 2:
        raise RecursionStop("Mergesort recursion completed successfully!")

    middle = len(inp) // 2
    left = inp[:middle]
    right = inp[middle:]
    return left, right


def merge(left: list[int], right: list[int]) -> list[int]:
    """Merge two sorted lists into a sorted list."""
    out = []
    # move through both lists, doing comparisons
    while len(left) and len(right):
        if left[0] < right[0]:
            out += [left.pop(0)]
        else:
            out += [right.pop(0)]

    # append any elements left
    out += left + right
    return out


def ignore_error(value: Result[T], default: Result[T]) -> T:
    """Pattern match on error values."""
    if isinstance(value, Error):
        return f.errors.unwrap(default)
    else:
        return value


def funsies_mergesort(art: Artefact[list[int]]) -> Artefact[list[int]]:
    """Mergesort a list of numbers with funsies."""
    result = f.dynamic.sac(
        # Recursive application of merge sort
        # split -> generates two list or raises
        # recurse(x) for each values of split
        # merge(left, right)
        split,
        lambda element: funsies_mergesort(element),
        lambda lr: f.reduce(merge, lr[0], lr[1]),  # type:ignore
        art,
        out=Encoding.json,
    )
    return f.reduce(
        # if the subdag fails, it's because split raised. In this case, we
        # just forward the arguments
        ignore_error,
        result,
        art,
        strict=False,
        out=Encoding.json,
    )


# run the workflow
to_be_sorted = [random.randint(0, 99) for _ in range(120)]
with f.Fun():
    inp = f.put(to_be_sorted)
    out = funsies_mergesort(inp)
    print("output:", out.hash)
    f.execute(out)
    f.wait_for(out)
    print(f.take(out))
