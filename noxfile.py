"""Automated testing linting and formatting apparatus."""
# external
import nox
from nox.sessions import Session

package = "funsies"
nox.options.sessions = "fmt", "lint", "tests", "mypy"  # default session
locations = "src", "tests", "noxfile.py", "recipes"  # Linting locations
pyversions = ["3.7", "3.8", "3.9"]

# to run darglint
# nox -rs darglint

# to run all tests:
# nox -rs tests -- --cov


# Testing
@nox.session(python=pyversions)
def tests(session: Session) -> None:
    """Run tests."""
    args = session.posargs or ["--cov", "-m not slow"]
    session.install("-r", "requirements.txt")
    session.install("pytest", "pytest-cov")
    session.install("-e", ".")
    session.run("pytest", *args)


# Linting
@nox.session(python="3.9")
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
    )
    session.run("flake8", *args)


# Code formatting
@nox.session(python="3.9")
def fmt(session: Session) -> None:
    """Format code."""
    args = session.posargs or locations
    session.install("black")
    session.install("isort")
    session.run("isort", *args)
    session.run("black", *args)


# Static typing
@nox.session(python="3.9")
def mypy(session: Session) -> None:
    """Run the static type checker."""
    args = session.posargs or locations
    session.install("mypy")
    session.install("types-redis")
    session.run("mypy", *args)


# Documentation
@nox.session(python="3.9")
def docs(session: Session) -> None:
    """Make documentation."""
    session.install("-r", "requirements.txt")
    session.install("-e", ".")
    session.install("pdoc3")
    session.run("rm", "-rfd", "docs", external=True)
    session.run("pdoc", "--template-dir", "src/templates", "--html", "funsies")
    session.run("mv", "html/funsies", "docs", external=True)


# Linting docstrings (slow...)
@nox.session(python="3.9")
def darglint(session: Session) -> None:
    """Lint docstrings arguments (slow)."""
    args = session.posargs or locations
    session.install("darglint")
    session.run("darglint", *args)
