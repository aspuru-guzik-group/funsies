"""Test of Funsies template()."""
# std

# funsies
from funsies import Fun, put, take, template
from funsies._context import get_connection
from funsies._run import run_op
from funsies.config import MockServer


def test_template() -> None:
    """Basic test of chevron templating."""
    with Fun(MockServer()):
        db, store = get_connection()
        t = "Hello, {{ mustache }}!"
        result = template(t, {"mustache": "world"})
        run_op(db, store, result.parent)
        assert take(result) == b"Hello, world!"


def test_template_complicated() -> None:
    """Test templating with funky types."""
    with Fun(MockServer()):
        db, store = get_connection()
        t = "wazzaa, {{ mustache }}!"
        result = template(t, {"mustache": put(b"people")})
        run_op(db, store, result.parent)
        assert take(result) == b"wazzaa, people!"

        t = "{{a}}{{b}}{{c}}"
        result = template(t, dict(a=2, b="cool", c="4me"))
        run_op(db, store, result.parent)
        assert take(result) == b"2cool4me"

        t = ""
        result = template(t, dict(a=2, b="cool", c="4me"))
        run_op(db, store, result.parent)
        assert take(result) == b""
