"""Test of Funsies datastructures."""
# module
from funsies import _funsies as f


def test_instantiate() -> None:
    """Test the instantiation of a Funsie class."""
    out = f.Funsie(
        how=f.FunsieHow.shell, what=b"cat", inp=["infile"], out=["outfile"], extra={}
    )
    assert out is not None


def test_str() -> None:
    """Test funsies to string."""
    out = f.Funsie(
        how=f.FunsieHow.shell,
        what=b"cat",
        inp=["infile1", "infile2"],
        out=["out2", "stdout"],
        extra={},
    )

    out2 = f.Funsie(
        how=f.FunsieHow.shell,
        what=b"cat",
        inp=["infile2", "infile1"],
        out=["out2", "stdout"],
        extra={},
    )

    out3 = f.Funsie(
        how=f.FunsieHow.python,
        what=b"cat",
        inp=["infile2", "infile1"],
        out=["out2", "stdout"],
        extra={},
    )

    assert str(out) == str(out2)
    assert str(out) != str(out3)
