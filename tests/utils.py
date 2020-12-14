"""Utility functions for tests."""
from typing import Optional


def assert_file(inp: Optional[bytes], equals: bytes) -> None:
    """Test whether a file is there and equal to a value."""
    assert inp is not None
    assert inp == equals
