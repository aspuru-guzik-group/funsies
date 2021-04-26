"""Serializers and deserializers for Encoding."""
from __future__ import annotations

# std
import json
import traceback
from typing import Any, Optional

# module
from ._constants import Encoding, hash_t
from .errors import Error, ErrorKind, Result


def decode(
    enc: Encoding,
    data: Result[bytes],
    carry_error: Optional[hash_t] = None,
) -> Result[Any]:
    """Deserialize data from bytes according to an Encoding."""
    if isinstance(data, Error):
        # forward errors
        return data

    if not isinstance(data, bytes):
        # duck typing classic
        return Error(  # type:ignore
            kind=ErrorKind.WrongType,
            details=f"attempted to decode value of type {type(data)} "
            + f"with decoder for {enc}",
            source=carry_error,
        )

    if enc == Encoding.blob:
        return data
    else:
        if enc == Encoding.json:
            try:
                return json.loads(data.decode())
            except Exception:
                tb_exc = traceback.format_exc()
                return Error(
                    kind=ErrorKind.JSONDecodingError, details=tb_exc, source=carry_error
                )
        else:
            return Error(
                kind=ErrorKind.UnknownEncodingError,
                details=f"encoding {enc} is not implemented.",
                source=carry_error,
            )


def encode(enc: Encoding, data: Result[object]) -> Result[bytes]:
    """Serialize data into bytes according to an Encoding."""
    if isinstance(data, Error):
        return data

    if enc == Encoding.blob:
        if not isinstance(data, bytes):
            # duck typing classic
            return Error(
                kind=ErrorKind.WrongType,
                details=f"attempted to encode value of type {type(data)} "
                + f"with encoder for {enc}",
            )
        else:
            return data

    elif enc == Encoding.json:
        try:
            return json.dumps(data).encode()
        except Exception:
            tb_exc = traceback.format_exc()
            return Error(kind=ErrorKind.JSONEncodingError, details=tb_exc)
    else:
        return Error(
            kind=ErrorKind.UnknownEncodingError,
            details=f"encoding {enc} is not implemented.",
        )


def kind(data: Any) -> Encoding:
    """Autodetect the correct encoder."""
    if isinstance(data, bytes):
        return Encoding.blob
    else:
        return Encoding.json
