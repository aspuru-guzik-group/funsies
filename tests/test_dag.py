"""Test of Funsies shell capabilities."""
# std
import time

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import dag, Fun, morph, put, shell, take


def test_dag_build() -> None:
    """Test simple DAG build."""
    with Fun(Redis()) as db:
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        output = step2.stdout

        dag_of = dag.build_dag(db, output.hash)
        assert dag_of is not None
        assert len(db.smembers(dag_of + ".root")) == 2

        # test deletion
        dag.delete_dag(db, output.hash)
        assert len(db.smembers(dag_of + ".root")) == 0

        # test new dag
        dag_of = dag.build_dag(db, step1.hash)
        assert dag_of is not None
        assert len(db.smembers(dag_of + ".root")) == 1


def test_dag_execute() -> None:
    """Test execution of a dag."""
    with Fun(Redis()):
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        output = step2.stdout

        # make queue
        dag.execute(output, queue_args=dict(is_async=False))
        out = take(output)
        time.sleep(0.1)
        assert out == b"BLA BLAbla bla"


def test_dag_execute2() -> None:
    """Test execution of a dag."""
    with Fun(Redis()):
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        step11 = shell("echo 'bla'")
        final = shell(
            "cat file1 file2", inp={"file1": step11.stdout, "file2": step2.stdout}
        )
        output = final.stdout

        # make queue
        dag.execute(output, queue_args=dict(is_async=False))
        out = take(output)
        assert out == b"bla\nBLA BLAbla bla"
