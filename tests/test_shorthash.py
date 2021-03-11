"""Test of artefacts save / restore."""
# std
import hashlib

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# funsies
from funsies import _short_hash as sh
from funsies.types import hash_t


def test_shorten_hash():
    """Test hash shortening."""
    m = hashlib.sha1()
    m.update("hi 👋 I am a sha 1 hash".encode())
    val = m.hexdigest()
    h = hash_t(val)
    short_hash = sh.shorten_hash(h)
    assert short_hash == h[:6]


def test_get_short_hash():
    """Test short hash saving and loading."""
    m = hashlib.sha1()
    m.update("hi 👋 I am a sha 1 hash".encode())
    val = m.hexdigest()

    m = hashlib.sha1()
    m.update("hi 👋  am another sha 1 hash".encode())
    val2 = m.hexdigest()

    # set up a hash that collides with val
    val_collide = val[:6] + "b" * (len(val) - 6)
    print(val)
    print(val2)
    print(val_collide)

    db = Redis()
    sh.hash_save(db, hash_t(val))
    sh.hash_save(db, hash_t(val_collide))
    sh.hash_save(db, hash_t(val2))

    dat = sh.hash_load(db, "")
    assert len(dat) == 3

    for i in range(1, 7):
        dat = sh.hash_load(db, val[:i])
        assert len(dat) == 2

    for i in range(7, len(val)):
        dat = sh.hash_load(db, val[:i])
        assert len(dat) == 1

    dat = sh.hash_load(db, val2)
    assert len(dat) == 1

    with pytest.raises(AttributeError):
        sh.hash_load(db, val + "x")
