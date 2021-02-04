"""Test of Funsies shell capabilities."""
# std

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import dag, execute, Fun, morph, options, put, shell, take
from funsies.constants import DAG_INDEX


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
    with Fun(Redis(), options(distributed=False)):
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        output = step2.stdout

        # make queue
        execute(output)
        out = take(output)
        assert out == b"BLA BLAbla bla"


def test_dag_execute2() -> None:
    """Test execution of a dag."""
    with Fun(Redis(), options(distributed=False)):
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        step11 = shell("echo 'bla'")
        final = shell(
            "cat file1 file2", inp={"file1": step11.stdout, "file2": step2.stdout}
        )
        output = final.stdout

        # make queue
        execute(output)
        out = take(output)
        assert out == b"bla\nBLA BLAbla bla"


def test_dag_execute_same_root() -> None:
    """Test execution of two dags that share the same origin."""
    with Fun(Redis(), options(distributed=False)):
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        step2b = shell("cat file1", inp=dict(file1=step1))

        execute(step2)
        out = take(step2.stdout)
        assert out == b"BLA BLAbla bla"

        execute(step2b)
        out = take(step2b.stdout)
        assert out == b"BLA BLA"


def test_dag_cleanup() -> None:
    """Test execution of two dags that share the same origin."""
    with Fun(Redis(), options(distributed=False)) as db:
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))

        execute(step2)
        out = take(step2.stdout)
        assert out == b"BLA BLAbla bla"

    with Fun(db, options(distributed=False), cleanup=False):
        assert len(db.smembers(DAG_INDEX)) == 1

    with Fun(db, options(distributed=False), cleanup=True):
        assert len(db.smembers(DAG_INDEX)) == 0
