"""Funsies is a transparently-memoized worfklow engine."""
# from .ui import file, pyfunc, shell

__all__ = [
    # ui
    # "file",
    # "shell",
    # "pyfunc",
]

# versioning information
try:
    from importlib import metadata

    __version__ = metadata.version("funsies")
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata

    __version__ = importlib_metadata.version("funsies")
