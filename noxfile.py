"""Automated testing linting and formatting apparatus."""
# external
import nox
from nox.sessions import Session

package = "funsies"
nox.options.sessions = "lint", "tests", "typeguard", "mypy"  # default session
locations = "src", "tests", "noxfile.py"  # Linting locations
pyversions = ["3.8"]


# Testing
@nox.session(python=pyversions)
def tests(session: Session) -> None:
    """Run tests."""
    args = session.posargs or ["--cov", "-m", "not prod"]
    session.install("-r", "requirements.txt")
    session.install("pytest", "pytest-cov")
    session.install("-e", ".", "--no-deps")
    session.run("pytest", *args)


# Linting
@nox.session(python="3.8")
def lint(session: Session) -> None:
    """Lint code."""
    args = session.posargs or locations
    session.install(
        "flake8",
        "flake8-black",
        "flake8-bugbear",
        "flake8-import-order",
        "flake8-annotations",
        "flake8-docstrings",
        "darglint",
    )
    session.run("flake8", *args)


# Code formatting
@nox.session(python="3.8")
def black(session: Session) -> None:
    """Format code."""
    args = session.posargs or locations
    session.install("black")
    session.run("black", *args)


# Static typing
@nox.session(python="3.8")
def mypy(session: Session) -> None:
    """Run the static type checker."""
    args = session.posargs or locations
    session.install("mypy")
    session.run("mypy", *args)


# Typeguard
@nox.session(python=pyversions)
def typeguard(session: Session) -> None:
    """Run tests."""
    args = session.posargs or []
    session.install("-r", "requirements.txt")
    session.install("pytest", "typeguard")
    session.install("-e", ".", "--no-deps")
    session.run("pytest", f"--typeguard-packages={package}", *args)
