"""Test of Funsies datastructures."""

# external
from funsies import _shell as s


def test_shell_wrap() -> None:
    """Test the instantiation of a Funsie class."""
    out = s.wrap_shell(["cat file1"], "file1", [])
    assert out is not None
