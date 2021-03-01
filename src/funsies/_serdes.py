"""Serializers and deserializers for Encoding."""
from __future__ import annotations

import json
import traceback
from typing import Any, cast, Optional

# module
from ._constants import Encoding, hash_t
from .errors import Error, ErrorKind, Result


def decode(
    enc: Encoding,
    data: Result[bytes],
    carry_error: Optional[hash_t] = None,
) -> Result[Any]:
    """Deserialize data from bytes according to an Encoding."""
    if enc == Encoding.blob:
        return data
    else:
        if isinstance(data, Error):
            return data

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
    if enc == Encoding.blob:
        return cast(Result[bytes], data)
    else:
        if isinstance(data, Error):
            return data

        if enc == Encoding.json:
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
