"""Test of Funsies shell capabilities."""
# external
from fakeredis import FakeStrictRedis as Redis
import rq
from rq import Queue
import time

# module
from funsies import dag
from funsies import take, morph, put, shell


def test_dag_build() -> None:
    """Test simple DAG build."""
    db = Redis()
    dat = put(db, "bla bla")
    step1 = morph(db, dat, lambda x: x.decode().upper().encode())
    step2 = shell(db, "cat file1 file2", inp=dict(file1=step1, file2=dat))
    output = step2.stdout

    dag_of = dag.build_dag(db, output.hash)
    assert len(db.smembers(dag_of + ".root")) == 2

    # test deletion
    dag.delete_dag(db, output.hash)
    assert len(db.smembers(dag_of + ".root")) == 0

    # test new dag
    dag_of = dag.build_dag(db, step1.hash)
    assert len(db.smembers(dag_of + ".root")) == 1


def test_dag_execute() -> None:
    """Test execution of a dag."""
    db = Redis()
    dat = put(db, "bla bla")
    step1 = morph(db, dat, lambda x: x.decode().upper().encode())
    step2 = shell(db, "cat file1 file2", inp=dict(file1=step1, file2=dat))
    output = step2.stdout

    # make queue
    queue = Queue(connection=db, is_async=False)
    dag.execute(db, queue, output)
    out = take(db, output)
    time.sleep(0.1)
    assert out == b"BLA BLAbla bla"
