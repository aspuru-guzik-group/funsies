"""Test serialization/deserialization."""
from funsies import _serdes
from funsies.types import Encoding, Error


def test_serde_blob() -> None:
    """Test 'blob' ser/deser."""
    assert _serdes.encode(Encoding.blob, b"bla") == b"bla"
    assert isinstance(_serdes.encode(Encoding.blob, "bla bla bla"), Error)
