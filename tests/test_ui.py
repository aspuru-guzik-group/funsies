"""Tests of user-friendly routines."""
import funsies


def test_arg_parsing() -> None:
    """Test various kind of argument parsing."""
    task = funsies.make("echo 123")
    task = funsies.make(*["echo 123", "echo 22"])
    task = funsies.make("echo 123", "echo 22")
    task = funsies.make("echo 123", ["echo", "11", "22"])
