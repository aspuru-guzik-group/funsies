"""Data for artefacts."""
# std
from typing import TypedDict


class Datum(TypedDict):
    """This is the main class that represents the data held by an artefact."""

    raw: bytes
