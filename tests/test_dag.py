"""Test of Funsies shell capabilities."""
# std

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import dag, execute, Fun, morph, options, put, shell, take
from funsies.constants import DAG_INDEX, DAG_STORE, hash_t
from funsies.utils import concat


def test_dag_build() -> None:
    """Test simple DAG build."""
    with Fun(Redis()) as db:
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        output = step2.stdout

        dag.build_dag(db, output.hash)
        assert len(db.smembers(DAG_STORE + "root")) == 1

        # test deletion
        dag.delete_all_dags(db)
        assert len(db.smembers(DAG_STORE + "root")) == 0

        # test new dag
        dag.build_dag(db, step1.hash)
        assert len(db.smembers(DAG_STORE + "root")) == 1


def test_dag_efficient() -> None:
    """Test that DAG building doesn't do extra work."""
    with Fun(Redis()) as db:
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell(
            "cat file1 file2", inp=dict(file1=step1, file2=dat), out=["file2"]
        )
        step2b = shell("echo 'not'", inp=dict(file1=step1))
        output = step2.stdout
        merge = shell(
            "cat file1 file2", inp=dict(file1=step1, file2=step2b.stdout), out=["file2"]
        )

        dag.build_dag(db, output.hash)
        # check that step2 only has stdout has dependent
        assert len(dag._dag_dependents(db, step1.parent)) == 1

        dag.delete_all_dags(db)
        dag.build_dag(db, merge.hash)
        # check that however, the merged one has two dependents for step1
        assert len(dag._dag_dependents(db, step1.parent)) == 2


def test_dag_cached() -> None:
    """Test that DAG caching."""
    with Fun(Redis(), options(distributed=False)):
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell(
            "cat file1 file2", inp=dict(file1=step1, file2=dat), out=["file2"]
        )
        step2b = shell("echo 'not'", inp=dict(file1=step1))
        output = step2.stdout
        merge = shell(
            "cat file1 file2", inp=dict(file1=step1, file2=step2b.stdout), out=["file2"]
        )
        execute(merge)
        execute(output)


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
    """Test dag cleanups."""
    with Fun(Redis(), options(distributed=False)) as db:
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))

        execute(step2)
        out = take(step2.stdout)
        assert out == b"BLA BLAbla bla"

    with Fun(db, options(distributed=False), cleanup=False):
        assert len(db.smembers(DAG_INDEX)) == 2

    with Fun(db, options(distributed=False), cleanup=True):
        assert len(db.smembers(DAG_INDEX)) == 0


def test_dag_large() -> None:
    """Test that DAG building doesn't do extra work for large operations."""
    with Fun(Redis()) as db:
        outputs = []
        for i in range(100):
            dat = put(f"bla{i}")
            step1 = morph(lambda x: x.decode().upper().encode(), dat)
            step2 = shell(
                "cat file1 file2",
                inp=dict(file1=step1, file2="something"),
                out=["file2"],
            )
            outputs += [concat(step1, step1, step2.stdout, join=" ")]

        final = concat(*outputs, join="\n")
        dag.build_dag(db, final.hash)
        assert len(dag._dag_dependents(db, hash_t("root"))) == 100
