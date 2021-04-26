"""Test of Funsies template()."""
# std

# external
from fakeredis import FakeStrictRedis as Redis

# funsies
from funsies import Fun, put, take, template
from funsies._run import run_op


def test_template() -> None:
    """Basic test of chevron templating."""
    with Fun(Redis()) as db:
        t = "Hello, {{ mustache }}!"
        result = template(t, {"mustache": "world"})
        run_op(db, result.parent)
        assert take(result) == b"Hello, world!"


def test_template_complicated() -> None:
    """Test templating with funky types."""
    with Fun(Redis()) as db:
        t = "wazzaa, {{ mustache }}!"
        result = template(t, {"mustache": put(b"people")})
        run_op(db, result.parent)
        assert take(result) == b"wazzaa, people!"

        t = "{{a}}{{b}}{{c}}"
        result = template(t, dict(a=2, b="cool", c="4me"))
        run_op(db, result.parent)
        assert take(result) == b"2cool4me"

        t = ""
        result = template(t, dict(a=2, b="cool", c="4me"))
        run_op(db, result.parent)
        assert take(result) == b""
